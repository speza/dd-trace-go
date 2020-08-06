// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

package tracer

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

type traceWriter interface {
	// stop shuts down the traceWriter, ensuring all payloads are flushed.
	stop()
}

type agentTraceWriter struct {
	config *config

	payload *payload

	// climit limits the number of concurrent outgoing connections
	climit chan struct{}

	// wg waits for all goroutines to exit when stopping.
	wg sync.WaitGroup

	// prioritySampling refers to the tracer's priority sampler.
	prioritySampling *prioritySampler

	// stopChan gets closed when stop() is called
	stopChan chan struct{}

	// traceChan receives traces to be added to the payload.
	traceChan chan []*span
}

var _ traceWriter = &agentTraceWriter{}

func newAgentTraceWriter(c *config, s *prioritySampler, traceChan chan []*span) *agentTraceWriter {
	atw := &agentTraceWriter{
		config:           c,
		payload:          newPayload(),
		climit:           make(chan struct{}, concurrentConnectionLimit),
		prioritySampling: s,
		stopChan:         make(chan struct{}, 1),
		traceChan:        traceChan,
	}
	go atw.run()
	atw.wg.Add(1)
	return atw
}

func (h *agentTraceWriter) run() {
	defer h.wg.Done()
	tick := h.config.tickChan
	if tick == nil {
		ticker := time.NewTicker(flushInterval)
		tick = ticker.C
	}
	for {
		select {
		case trace := <-h.traceChan:
			h.add(trace)
		case <-tick:
			h.config.statsd.Incr("datadog.tracer.flush_triggered", []string{"reason:scheduled"}, 1)
			h.flush()
		case <-h.stopChan:
		loop:
			// the loop ensures that the payload channel is fully drained
			// before the final flush to ensure no traces are lost (see #526)
			for {
				select {
				case trace := <-h.traceChan:
					h.add(trace)
				default:
					break loop
				}
			}
			h.flush()
			h.config.statsd.Incr("datadog.tracer.stopped", nil, 1)
			return
		}
	}
}

func (h *agentTraceWriter) add(trace []*span) {
	if err := h.payload.push(trace); err != nil {
		h.config.statsd.Incr("datadog.tracer.traces_dropped", []string{"reason:encoding_error"}, 1)
		log.Error("error encoding msgpack: %v", err)
	}
	if h.payload.size() > payloadSizeLimit {
		h.config.statsd.Incr("datadog.tracer.flush_triggered", []string{"reason:size"}, 1)
		h.flush()
	}
}

func (h *agentTraceWriter) stop() {
	h.config.statsd.Incr("datadog.tracer.flush_triggered", []string{"reason:shutdown"}, 1)
	select {
	case h.stopChan <- struct{}{}:
	default:
	}
	h.wg.Wait()
}

// flush will push any currently buffered traces to the server.
func (h *agentTraceWriter) flush() {
	if h.payload.itemCount() == 0 {
		return
	}
	h.wg.Add(1)
	h.climit <- struct{}{}
	go func(p *payload) {
		defer func(start time.Time) {
			<-h.climit
			h.wg.Done()
			h.config.statsd.Timing("datadog.tracer.flush_duration", time.Since(start), nil, 1)
		}(time.Now())
		size, count := p.size(), p.itemCount()
		log.Debug("Sending payload: size: %d traces: %d\n", size, count)
		rc, err := h.config.transport.send(p)
		if err != nil {
			h.config.statsd.Count("datadog.tracer.traces_dropped", int64(count), []string{"reason:send_failed"}, 1)
			log.Error("lost %d traces: %v", count, err)
		} else {
			h.config.statsd.Count("datadog.tracer.flush_bytes", int64(size), nil, 1)
			h.config.statsd.Count("datadog.tracer.flush_traces", int64(count), nil, 1)
			if err := h.prioritySampling.readRatesJSON(rc); err != nil {
				h.config.statsd.Incr("datadog.tracer.decode_error", nil, 1)
			}
		}
	}(h.payload)
	h.payload = newPayload()
}

type hexInt uint64

func (i hexInt) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatUint(uint64(i), 16) + `"`), nil
}

type jsonSpan struct {
	TraceID  hexInt             `json:"trace_id"`
	SpanID   hexInt             `json:"span_id"`
	ParentID hexInt             `json:"parent_id"`
	Name     string             `json:"name"`
	Resource string             `json:"resource"`
	Error    int32              `json:"error"`
	Meta     map[string]string  `json:"meta"`
	Metrics  map[string]float64 `json:"metrics"`
	Start    int64              `json:"start"`
	Duration int64              `json:"duration"`
	Service  string             `json:"service"`
}
type jsonPayload struct {
	Traces [][]jsonSpan `json:"traces"`
}

type lambdaTraceWriter struct {
	config  *config
	payload jsonPayload

	// wg waits for all goroutines to exit when stopping.
	wg sync.WaitGroup

	// stopChan gets closed when stop() is called
	stopChan chan struct{}

	// traceChan receives traces to be added to the payload.
	traceChan chan []*span
}

var _ traceWriter = &lambdaTraceWriter{}

func newLambdaTraceWriter(c *config, traceChan chan []*span) *lambdaTraceWriter {
	ltw := &lambdaTraceWriter{
		config:    c,
		stopChan:  make(chan struct{}, 1),
		traceChan: traceChan,
	}

	go ltw.run()
	ltw.wg.Add(1)
	return ltw
}

func (h *lambdaTraceWriter) run() {
	defer h.wg.Done()
	tick := h.config.tickChan
	if tick == nil {
		ticker := time.NewTicker(flushInterval)
		tick = ticker.C
	}
	for {
		select {
		case trace := <-h.traceChan:
			h.add(trace)
		case <-tick:
			h.config.statsd.Incr("datadog.tracer.flush_triggered", []string{"reason:scheduled"}, 1)
			h.flush()
		case <-h.stopChan:
		loop:
			// the loop ensures that the payload channel is fully drained
			// before the final flush to ensure no traces are lost (see #526)
			for {
				select {
				case trace := <-h.traceChan:
					h.add(trace)
				default:
					break loop
				}
			}
			h.flush()
			h.config.statsd.Incr("datadog.tracer.stopped", nil, 1)
			return
		}
	}
}

func (h *lambdaTraceWriter) add(trace []*span) {
	js := make([]jsonSpan, len(trace))
	for i, s := range trace {
		js[i] = jsonSpan{
			TraceID:  hexInt(s.TraceID),
			SpanID:   hexInt(s.SpanID),
			ParentID: hexInt(s.ParentID),
			Name:     s.Name,
			Resource: s.Resource,
			Error:    s.Error,
			Meta:     s.Meta,
			Metrics:  s.Metrics,
			Start:    s.Start,
			Duration: s.Duration,
			Service:  s.Service,
		}
	}
	h.payload.Traces = append(h.payload.Traces, js)
}

func (h *lambdaTraceWriter) stop() {
	h.config.statsd.Incr("datadog.tracer.flush_triggered", []string{"reason:shutdown"}, 1)
	select {
	case h.stopChan <- struct{}{}:
	default:
	}
	h.wg.Wait()
}

// flush will push any currently buffered traces to the server.
func (h *lambdaTraceWriter) flush() {
	bs, err := json.Marshal(h.payload)
	if err != nil {
		h.config.statsd.Count("datadog.tracer.traces_dropped", int64(len(h.payload.Traces)), []string{"reason:encoding_failed"}, 1)
		log.Error("lost %d traces: %v", len(h.payload.Traces), err)
		h.payload.Traces = h.payload.Traces[:0]
		return
	}
	h.payload.Traces = h.payload.Traces[:0]
	log.Info("%s", string(bs))
}
