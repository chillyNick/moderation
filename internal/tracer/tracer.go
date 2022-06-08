package tracer

import (
	"bytes"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/homework3/moderation/internal/config"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"io"

	"github.com/rs/zerolog/log"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

func NewTracer(cfg *config.Config) (io.Closer, error) {
	cfgTracer := &jaegercfg.Configuration{
		ServiceName: cfg.Jaeger.Service,
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans:           true,
			LocalAgentHostPort: cfg.Jaeger.Host + ":" + cfg.Jaeger.Port,
		},
	}
	tracer, closer, err := cfgTracer.NewTracer(jaegercfg.Logger(jaeger.StdLogger))
	if err != nil {
		log.Error().Err(err).Msgf("failed init jaeger: %v", err)

		return nil, err
	}
	opentracing.SetGlobalTracer(tracer)
	log.Info().Msgf("Traces started")

	return closer, nil
}

var headerKey = []byte("trace")

func ExtractSpanContext(headers []*sarama.RecordHeader) (opentracing.SpanContext, error) {
	var traceHeader []byte
	for _, v := range headers {
		if bytes.Compare(headerKey, v.Key) == 0 {
			traceHeader = v.Value
			break
		}
	}
	if len(traceHeader) == 0 {
		return nil, fmt.Errorf("trace not found in headers")
	}

	return opentracing.GlobalTracer().Extract(opentracing.Binary, bytes.NewReader(traceHeader))
}
