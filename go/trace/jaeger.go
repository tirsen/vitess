package trace

import (
  "flag"
  "io"
  "strconv"

  "github.com/opentracing/opentracing-go"
  "github.com/uber/jaeger-client-go"
  jaegercfg "github.com/uber/jaeger-client-go/config"
  "golang.org/x/net/context"
)

type JaegerSpan struct {
  otSpan opentracing.Span
}

func (js JaegerSpan) Finish() {
  js.otSpan.Finish()
}

func (js JaegerSpan) Annotate(key string, value interface{}) {
  js.otSpan.SetTag(key, value)
}

type OpenTracingFactory struct {
  Tracer opentracing.Tracer
}

var (
  tracingServer = flag.String("jaeger-agent-host", "", "host and port to send spans to. if empty, no tracing will be done")
  samplingRate  = flag.String("tracing-sampling-rate", "0.1", "sampling rate for the probabilistic jaeger sampler")
)

// SetJaegerTracingForService starts Jaeger tracing and registers it as the GlobalTracer
func SetJaegerTracingForService(serviceName string) io.Closer {

  probability, err := strconv.ParseFloat(*samplingRate, 32)
  if err != nil {
    probability = 0.1
  }

  cfg := jaegercfg.Configuration{
    Sampler: &jaegercfg.SamplerConfig{
      Type:  jaeger.SamplerTypeConst,
      Param: probability,
    },
    Reporter: &jaegercfg.ReporterConfig{
      LogSpans:           true,
      LocalAgentHostPort: *tracingServer,
    },
    RPCMetrics: false,
  }

  closer, err := cfg.InitGlobalTracer(serviceName)

  if err == nil {
    tracer := opentracing.GlobalTracer()

    tracerFactory := OpenTracingFactory{Tracer: tracer}
    RegisterSpanFactory(tracerFactory)
    return closer
  }

  return nil
}

func (jf OpenTracingFactory) New(parent Span, label string, spanType SpanType) Span {
  var innerSpan opentracing.Span
  if parent == nil {
    innerSpan = jf.Tracer.StartSpan(label)
  } else {
    jaegerParent := parent.(JaegerSpan)
    span := jaegerParent.otSpan
    innerSpan = jf.Tracer.StartSpan(label, opentracing.ChildOf(span.Context()))
  }
  return JaegerSpan{otSpan: innerSpan}
}

func (jf OpenTracingFactory) FromContext(ctx context.Context) (Span, bool) {
  innerSpan := opentracing.SpanFromContext(ctx)

  if innerSpan != nil {
    return JaegerSpan{otSpan: innerSpan}, true
  } else {
    return nil, false
  }
}

func (jf OpenTracingFactory) NewContext(parent context.Context, s Span) context.Context {
  span, ok := s.(JaegerSpan)
  if !ok {
    return nil
  }
  return opentracing.ContextWithSpan(parent, span.otSpan)
}
