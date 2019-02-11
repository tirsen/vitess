/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package trace contains a helper interface that allows various tracing
// tools to be plugged in to components using this interface. If no plugin is
// registered, the default one makes all trace calls into no-ops.
package trace

import (
	"golang.org/x/net/context"
)

// Span represents a unit of work within a trace. After creating a Span with
// NewSpan(), call one of the Start methods to mark the beginning of the work
// represented by this Span. Call Finish() when that work is done to record the
// Span. A Span may be reused by calling Start again.
type Span interface {
	// Finish marks the span as complete.
	Finish()
	// Annotate records a key/value pair associated with a Span. It should be
	// called between Start and Finish.
	Annotate(key string, value interface{})
}


// NewClientSpan returns a span and a context to register calls to dependent services
func NewClientSpan(inCtx context.Context, serviceName, spanLabel string) (Span, context.Context) {
	span, ctx := NewSpan(inCtx, spanLabel, Client)
	span.Annotate("peer.service", serviceName)
	return span, ctx
}

// NewSpan returns a span and a context - the context containing said span
func NewSpan(inCtx context.Context, label string, spanType SpanType) (Span, context.Context) {
	parent, _ := spanFactory.FromContext(inCtx)
	span := spanFactory.New(parent, label, spanType)
	outCtx := spanFactory.NewContext(inCtx, span)

	return span, outCtx
}

const extractSize = 10

// ExtractFirstCharacters returns the first few characters of a string.
// If the string had to be truncated, "..." is added to the end
func ExtractFirstCharacters(in string) string {
	if len(in) < extractSize {
		return in
	}
	runes := []rune(in)
	return string(runes[0:extractSize]) + "..."
}

// CopySpan creates a new context from parentCtx, with only the trace span
// copied over from spanCtx, if it has any. If not, parentCtx is returned.
func CopySpan(parentCtx, spanCtx context.Context) context.Context {
	if span, ok := spanFactory.FromContext(spanCtx); ok {
		return spanFactory.NewContext(parentCtx, span)
	}
	return parentCtx
}

type SpanType int

const (
	Local SpanType = iota
	Client
	Server
)

// SpanFactory is an interface for creating spans or extracting them from Contexts.
type SpanFactory interface {
	New(parent Span, label string, spanType SpanType) Span
	FromContext(ctx context.Context) (Span, bool)
	NewContext(parent context.Context, span Span) context.Context
}

var spanFactory SpanFactory = fakeSpanFactory{}

// RegisterSpanFactory should be called by a plugin during init() to install a
// factory that creates Spans for that plugin's tracing framework. Each call to
// RegisterSpanFactory will overwrite any previous setting. If no factory is
// registered, the default fake factory will produce Spans whose methods are all
// no-ops.
func RegisterSpanFactory(sf SpanFactory) {
	spanFactory = sf
}

type fakeSpanFactory struct{}

func (fakeSpanFactory) New(Span, string, SpanType) Span                                                 { return fakeSpan{} }
func (fakeSpanFactory) FromContext(context.Context) (Span, bool)                                        { return nil, false }
func (fakeSpanFactory) NewContext(parent context.Context, _ Span) context.Context { return parent }

// fakeSpan implements Span with no-op methods.
type fakeSpan struct{}

func (fakeSpan) Finish()                      {}
func (fakeSpan) Annotate(string, interface{}) {}
