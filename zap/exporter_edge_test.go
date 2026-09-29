// logx/zap/exporter_edge_test.go
// exporter 构建路由边界单测：buildSampler 全类型、buildSpanExporter 未知类型报错、
// buildMQExporter 缺 brokers、newJaegerExporter agent/endpoint 回退、newOTLPExporter 请求头
package zap

import (
	"testing"

	"github.com/gospacex/logx"
)

// TestBuildSamplerAllTypes 采样器全路由：显式类型 + 未知类型回退 ParentBased
func TestBuildSamplerAllTypes(t *testing.T) {
	for _, typ := range []string{"always", "never", "traceidratio", "parentbased_traceidratio", "unknown-type"} {
		if s := buildSampler(logx.SamplerConfig{Type: typ, Ratio: 0.5}); s == nil {
			t.Errorf("buildSampler(%q) = nil, want 非 nil sampler", typ)
		}
	}
	// 未知类型走 default 分支（ParentBased 包装，非 nil）
	if s := buildSampler(logx.SamplerConfig{Type: "whatever"}); s == nil {
		t.Error("未知类型应回退 ParentBased（非 nil）")
	}
}

// TestBuildSpanExporterUnsupported 未知 exporter 类型 → 构造错误（非静默）
func TestBuildSpanExporterUnsupported(t *testing.T) {
	_, err := buildSpanExporter(logx.OTelConfig{Exporter: "bogus"}, logx.MQConfig{})
	if err == nil {
		t.Fatal("buildSpanExporter(bogus) 未报错")
	}
}

// TestBuildMQExporterNoBrokers 缺 brokers 时 fail fast（kafka 路由前置校验）
func TestBuildMQExporterNoBrokers(t *testing.T) {
	if _, err := buildMQExporter(logx.MQConfig{}); err == nil {
		t.Error("buildMQExporter 缺 brokers 未报错")
	}
}

// TestNewJaegerExporterAgentHostPort agent UDP 模式（带 host + port）构造成功
func TestNewJaegerExporterAgentHostPort(t *testing.T) {
	cfg := logx.OTelConfig{
		Jaeger: logx.JaegerConfig{AgentHost: "127.0.0.1", AgentPort: 6831},
	}
	if _, err := newJaegerExporter(cfg); err != nil {
		t.Errorf("agent 模式（host+port）构造报错: %v", err)
	}
}

// TestNewJaegerExporterTopLevelEndpointFallback Jaeger.Endpoint 为空时回退顶层 cfg.Endpoint（collector HTTP 分支）
func TestNewJaegerExporterTopLevelEndpointFallback(t *testing.T) {
	cfg := logx.OTelConfig{Endpoint: "http://127.0.0.1:14268/api/traces"}
	if _, err := newJaegerExporter(cfg); err != nil {
		t.Errorf("顶层 Endpoint 回退构造报错: %v", err)
	}
}

// TestNewOTLPExporterHeaders 请求头注入分支（Headers 非空时 WithHeaders 生效、构造成功）
func TestNewOTLPExporterHeaders(t *testing.T) {
	cfg := logx.OTLPConfig{
		Endpoint: "http://127.0.0.1:4318",
		Headers:  map[string]string{"x-tenant": "acme"},
	}
	if _, err := newOTLPExporter(cfg); err != nil {
		t.Errorf("带 Headers 的 OTLP exporter 构造报错: %v", err)
	}
}

// TestKafkaProducerClose 关闭 writer 无副作用（不触网；kafka-go 内部幂等）
func TestKafkaProducerClose(t *testing.T) {
	p, err := NewKafkaProducer(logx.MQConfig{Type: "kafka", Brokers: []string{"localhost:1"}, Topic: "t"})
	if err != nil {
		t.Fatalf("NewKafkaProducer: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close = %v, want nil", err)
	}
}
