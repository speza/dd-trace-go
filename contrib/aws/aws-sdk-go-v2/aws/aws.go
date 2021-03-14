// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package aws

import (
	"context"
	"fmt"
	"math"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	tagAWSAgent     = "aws.agent"
	tagAWSService   = "aws.service"
	tagAWSOperation = "aws.operation"
	tagAWSRegion    = "aws.region"
	tagAWSRequestID = "aws.request_id"
)

// Middleware allows us to add middleware to the AWS SDK GO v2.
// See https://aws.github.io/aws-sdk-go-v2/docs/middleware for more information.
type Middleware struct {
	cfg *config
}

// NewMiddleware creates a new Middleware.
func NewMiddleware(opts ...Option) *Middleware {
	cfg := &config{}

	defaults(cfg)
	for _, opt := range opts {
		opt(cfg)
	}

	return &Middleware{cfg: cfg}
}

// Append takes the API options from the aws.Config.
func (mw *Middleware) Append(apiOptions *[]func(*middleware.Stack) error) {
	*apiOptions = append(*apiOptions, func(stack *middleware.Stack) error {
		// Add Initialize middleware after existing AWS middleware to make sure we can get details from the context.
		// This also allows us to start the trace as early as possible.
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("TraceInitialize", mw.initialize), middleware.After)
	})
	*apiOptions = append(*apiOptions, func(stack *middleware.Stack) error {
		// Add Deserialize middleware to trace elements of the http request which is only built and sent at this point.
		return stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("TraceDeserialize", mw.deserialize), middleware.Before)
	})
}

func (mw *Middleware) initialize(
	ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler,
) (
	out middleware.InitializeOutput, metadata middleware.Metadata, err error,
) {
	region := awsmiddleware.GetRegion(ctx)
	operation := awsmiddleware.GetOperationName(ctx)
	serviceID := awsmiddleware.GetServiceID(ctx)

	opts := []ddtrace.StartSpanOption{
		tracer.SpanType(ext.SpanTypeHTTP),
		tracer.ServiceName(serviceName(mw.cfg, serviceID)),
		tracer.ResourceName(fmt.Sprintf("%s.%s", serviceID, operation)),
		tracer.Tag(tagAWSRegion, region),
		tracer.Tag(tagAWSOperation, operation),
		tracer.Tag(tagAWSService, serviceID),
	}
	if !math.IsNaN(mw.cfg.analyticsRate) {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, mw.cfg.analyticsRate))
	}
	span, spanctx := tracer.StartSpanFromContext(ctx, fmt.Sprintf("%s.request", serviceID), opts...)

	// Handle Initialize and continue through the middleware chain.
	out, metadata, err = next.HandleInitialize(spanctx, in)
	if err != nil {
		span.Finish(tracer.WithError(err))
	} else {
		span.Finish()
	}

	return out, metadata, err
}

func (mw *Middleware) deserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (
	out middleware.DeserializeOutput, metadata middleware.Metadata, err error,
) {
	span, ok := tracer.SpanFromContext(ctx)
	if !ok {
		// If no span is found then we don't need to enrich the trace so just continue.
		return next.HandleDeserialize(ctx, in)
	}

	// Get values out of the request.
	if req, ok := in.Request.(*smithyhttp.Request); ok {
		span.SetTag(ext.HTTPMethod, req.Method)
		span.SetTag(ext.HTTPURL, req.URL.String())
		span.SetTag(tagAWSAgent, req.Header.Get("User-Agent"))
	}

	// Continue through the middleware layers, eventually sending the request.
	out, metadata, err = next.HandleDeserialize(ctx, in)

	// Get values out of the response.
	if res, ok := out.RawResponse.(*smithyhttp.Response); ok {
		span.SetTag(ext.HTTPCode, res.StatusCode)
	}

	// Extract the request id.
	if requestID, ok := awsmiddleware.GetRequestIDMetadata(metadata); ok {
		span.SetTag(tagAWSRequestID, requestID)
	}

	return out, metadata, err
}

func serviceName(cfg *config, serviceID string) string {
	if cfg.serviceName != "" {
		return cfg.serviceName
	}

	return fmt.Sprintf("aws.%s", serviceID)
}
