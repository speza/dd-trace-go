// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

package tracer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

type traceHandler interface {
	// pushPayload adds a trace to the current payload being constructed by the handler.
	pushPayload([]*span)
	// flush should cause the handler to send its current payload. The flush can happen asynchronously, but
	// the handler must be ready to accept new traces with pushPayload when flush() returns.
	flush()
	// wait will pause until the traceHandler is finished any asynchronous tasks it is performing.
	wait()
	// sample gives a handler the opportunity to perform sampling on a span. This is called when a span
	// is created.
	sample(*span)
}

type agentTraceHandler struct {
	payload *payload
	// statsd is used for tracking metrics associated with the runtime and the tracer.
	statsd statsdClient
	// climit limits the number of concurrent outgoing connections
	climit chan struct{}

	// wg waits for all goroutines to exit when stopping.
	wg sync.WaitGroup

	// transport specifies the Transport interface which will be used to send data to the agent.
	transport transport

	// prioritySampling refers to the tracer's priority sampler.
	prioritySampling *prioritySampler
}

var _ traceHandler = &agentTraceHandler{}

func (h *agentTraceHandler) pushPayload(trace []*span) {
	if err := h.payload.push(trace); err != nil {
		h.statsd.Incr("datadog.tracer.traces_dropped", []string{"reason:encoding_error"}, 1)
		log.Error("error encoding msgpack: %v", err)
	}
	if h.payload.size() > payloadSizeLimit {
		h.statsd.Incr("datadog.tracer.flush_triggered", []string{"reason:size"}, 1)
		h.flush()
	}
}

// flush will push any currently buffered traces to the server.
func (h *agentTraceHandler) flush() {
	if h.payload.itemCount() == 0 {
		return
	}
	h.wg.Add(1)
	h.climit <- struct{}{}
	go func(p *payload) {
		defer func(start time.Time) {
			<-h.climit
			h.wg.Done()
			h.statsd.Timing("datadog.tracer.flush_duration", time.Since(start), nil, 1)
		}(time.Now())
		size, count := p.size(), p.itemCount()
		log.Debug("Sending payload: size: %d traces: %d\n", size, count)
		rc, err := h.transport.send(p)
		if err != nil {
			h.statsd.Count("datadog.tracer.traces_dropped", int64(count), []string{"reason:send_failed"}, 1)
			log.Error("lost %d traces: %v", count, err)
		} else {
			h.statsd.Count("datadog.tracer.flush_bytes", int64(size), nil, 1)
			h.statsd.Count("datadog.tracer.flush_traces", int64(count), nil, 1)
			if err := h.prioritySampling.readRatesJSON(rc); err != nil {
				h.statsd.Incr("datadog.tracer.decode_error", nil, 1)
			}
		}
	}(h.payload)
	h.payload = newPayload()
}

func (h *agentTraceHandler) sample(s *span) {
	h.prioritySampling.apply(s)
}

func (h *agentTraceHandler) wait() {
	h.wg.Wait()
}

type hexInt uint64

func (i hexInt) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%X"`, i)), nil
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

type lambdaTraceHandler struct {
	payload jsonPayload
	// statsd is used for tracking metrics associated with the runtime and the tracer.
	statsd statsdClient
}

var _ traceHandler = &lambdaTraceHandler{}

func (h *lambdaTraceHandler) pushPayload(trace []*span) {
	log.Info("PUSHING PAYLOAD WITH %d SPANS", len(trace))
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

// flush will push any currently buffered traces to the server.
func (h *lambdaTraceHandler) flush() {
	log.Info("MARSHALING PAYLOAD WITH %d SPANS", len(h.payload.Traces))
	bs, err := json.Marshal(h.payload)

	if err != nil {
		h.statsd.Count("datadog.tracer.traces_dropped", int64(len(h.payload.Traces)), []string{"reason:encoding_failed"}, 1)
		log.Error("lost %d traces: %v", len(h.payload.Traces), err)
		h.payload.Traces = h.payload.Traces[:0]
		return
	}
	h.payload.Traces = h.payload.Traces[:0]
	log.Info("%s", string(bs))
}

func (h *lambdaTraceHandler) sample(*span) {}

func (h *lambdaTraceHandler) wait() {}
