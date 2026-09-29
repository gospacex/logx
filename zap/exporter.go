// logx/zap/exporter.go
// OTel trace exporter：构建 TracerProvider
// 支持直接推送到 Jaeger / OTLP / stdout，并设置为全局 TracerProvider
package zap

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gospacex/logx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	jaeger "go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TracerSetup OTel TracerProvider 装配结果
type TracerSetup struct {
	// Provider 全局 TracerProvider
	Provider *sdktrace.TracerProvider
	// Exporter 使用的导出器
	Exporter sdktrace.SpanExporter
}

// SetupTracerProvider 根据配置创建全局 OTel TracerProvider（kafka 路由需经 SetupTracerProviderWithMQ 传 MQConfig）
func SetupTracerProvider(cfg logx.OTelConfig) (*TracerSetup, error) {
	return SetupTracerProviderWithMQ(cfg, logx.MQConfig{})
}

// SetupTracerProviderWithMQ 统一装配入口：jaeger/otlp/stdout/kafka(别名 mq)
// kafka 路由需传 MQConfig；其余 exporter 忽略 mq 参数
// 调用后自动设置为 otel.SetTracerProvider 与全局 TextMapPropagator
func SetupTracerProviderWithMQ(cfg logx.OTelConfig, mq logx.MQConfig) (*TracerSetup, error) {
	cfg.FillDefaults()

	exp, err := buildSpanExporter(cfg, mq)
	if err != nil {
		return nil, err
	}

	res := resource.NewWithAttributes(
		"",
		attribute.String("service.name", cfg.ServiceName),
		attribute.String("service.version", cfg.ServiceVersion),
		attribute.String("deployment.environment", cfg.Environment),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(buildSampler(cfg.Sampler)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &TracerSetup{Provider: tp, Exporter: exp}, nil
}

// buildSpanExporter 构建 span 导出器（kafka 需传入 MQ 配置）
func buildSpanExporter(cfg logx.OTelConfig, mq logx.MQConfig) (sdktrace.SpanExporter, error) {
	switch cfg.Exporter {
	case "jaeger", "":
		return newJaegerExporter(cfg)
	case "otlp":
		return newOTLPExporter(cfg.OTLP)
	case "stdout":
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	case "kafka", "mq":
		return buildMQExporter(mq)
	default:
		return nil, fmt.Errorf("unsupported otel exporter: %s", cfg.Exporter)
	}
}

// buildMQExporter 构建 Kafka 直推 exporter（复用 exporter_mq.go 的 buildProducer/NewMQExporter）
// 缺 Brokers 时 fail fast
func buildMQExporter(mq logx.MQConfig) (sdktrace.SpanExporter, error) {
	if len(mq.Brokers) == 0 {
		return nil, fmt.Errorf("otel kafka exporter: no brokers configured; set logx.mq.brokers")
	}
	mq.FillDefaults()
	queue, err := buildProducer(mq)
	if err != nil {
		return nil, err
	}
	exp, err := NewMQExporter(queue)
	if err != nil {
		_ = queue.Close()
		return nil, err
	}
	return exp, nil
}

// newJaegerExporter 构建 Jaeger exporter
// 优先 collector HTTP 端点（如 http://localhost:14268/api/traces），
// 未配置时回退到 agent UDP（默认 localhost:6831）
func newJaegerExporter(cfg logx.OTelConfig) (*jaeger.Exporter, error) {
	endpoint := cfg.Jaeger.Endpoint
	if endpoint == "" {
		endpoint = cfg.Endpoint
	}

	if endpoint != "" {
		return jaeger.New(jaeger.WithCollectorEndpoint(
			jaeger.WithEndpoint(endpoint),
		))
	}

	// agent UDP 模式
	opts := []jaeger.AgentEndpointOption{}
	if cfg.Jaeger.AgentHost != "" {
		opts = append(opts, jaeger.WithAgentHost(cfg.Jaeger.AgentHost))
	}
	if cfg.Jaeger.AgentPort > 0 {
		opts = append(opts, jaeger.WithAgentPort(fmt.Sprintf("%d", cfg.Jaeger.AgentPort)))
	}
	return jaeger.New(jaeger.WithAgentEndpoint(opts...))
}

// newOTLPExporter 构建 OTLP HTTP exporter
func newOTLPExporter(cfg logx.OTLPConfig) (sdktrace.SpanExporter, error) {
	opts := []otlptracehttp.Option{}
	insecure := cfg.Insecure
	if cfg.Endpoint != "" {
		if strings.HasPrefix(cfg.Endpoint, "http://") || strings.HasPrefix(cfg.Endpoint, "https://") {
			// 完整 URL（如 http://localhost:4318）走 WithEndpointURL；
			// http scheme 必须 Insecure（非 TLS），否则 otlp 默认走 https 静默失联
			opts = append(opts, otlptracehttp.WithEndpointURL(cfg.Endpoint))
			if strings.HasPrefix(cfg.Endpoint, "http://") {
				insecure = true
			}
		} else {
			// host:port 形式（如 localhost:4318）；Insecure 由配置显式指定
			opts = append(opts, otlptracehttp.WithEndpoint(cfg.Endpoint))
		}
	}
	if insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(cfg.Headers))
	}
	return otlptracehttp.New(context.Background(), opts...)
}

// buildSampler 构建采样器
func buildSampler(s logx.SamplerConfig) sdktrace.Sampler {
	switch s.Type {
	case "always":
		return sdktrace.AlwaysSample()
	case "never":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(s.Ratio)
	default: // parentbased_traceidratio / parentbased
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(s.Ratio))
	}
}
