// logx/config_edge_test.go
// 配置填充边界：OTel 顶层继承优先级、OTelConfig/MQConfig 独立 FillDefaults 的
// 零值→默认 与 显式值保留语义
package logx

import (
	"testing"
	"time"
)

// TestOTelInheritanceTopLevelExplicit 顶层 ServiceName 已填后，OTel 独立值优先不被覆盖
func TestOTelInheritanceTopLevelExplicit(t *testing.T) {
	cfg := Config{
		ServiceName:    "top-svc",
		ServiceVersion: "9.9.9",
		Environment:    "prod",
		OTel: OTelConfig{
			ServiceName:    "otel-svc",
			ServiceVersion: "2.0.0",
			Environment:    "staging",
		},
	}
	cfg.FillDefaults()
	if cfg.OTel.ServiceName != "otel-svc" {
		t.Errorf("OTel.ServiceName = %q, want otel-svc（显式 OTel 值优先）", cfg.OTel.ServiceName)
	}
	if cfg.OTel.ServiceVersion != "2.0.0" {
		t.Errorf("OTel.ServiceVersion = %q, want 2.0.0", cfg.OTel.ServiceVersion)
	}
	if cfg.OTel.Environment != "staging" {
		t.Errorf("OTel.Environment = %q, want staging", cfg.OTel.Environment)
	}
}

// TestOTelInheritanceFromTopLevel 顶层有值、OTel 空 → 继承顶层（Config.FillDefaults 的继承分支）
func TestOTelInheritanceFromTopLevel(t *testing.T) {
	cfg := Config{
		ServiceName:    "top-svc",
		ServiceVersion: "9.9.9",
		Environment:    "prod",
	}
	cfg.FillDefaults()
	if cfg.OTel.ServiceName != "top-svc" {
		t.Errorf("OTel.ServiceName = %q, want 继承顶层 top-svc", cfg.OTel.ServiceName)
	}
	if cfg.OTel.ServiceVersion != "9.9.9" {
		t.Errorf("OTel.ServiceVersion = %q, want 继承 9.9.9", cfg.OTel.ServiceVersion)
	}
	if cfg.OTel.Environment != "prod" {
		t.Errorf("OTel.Environment = %q, want 继承 prod", cfg.OTel.Environment)
	}
}

// TestOTelConfigFillDefaults 独立填充：零值全默认 + 显式值保留
func TestOTelConfigFillDefaults(t *testing.T) {
	// 零值 → 全默认
	var o OTelConfig
	o.FillDefaults()
	if o.Exporter != "jaeger" || o.ServiceName != "logx" || o.ServiceVersion != "1.0.0" || o.Environment != "development" {
		t.Errorf("零值填充结果不符: exporter=%q svc=%q ver=%q env=%q", o.Exporter, o.ServiceName, o.ServiceVersion, o.Environment)
	}
	if o.Sampler.Type != "parentbased_traceidratio" || o.Sampler.Ratio != 1.0 {
		t.Errorf("sampler 零值填充不符: type=%q ratio=%v", o.Sampler.Type, o.Sampler.Ratio)
	}

	// 显式值保留
	o2 := OTelConfig{
		Exporter:       "kafka",
		ServiceName:    "svc-x",
		ServiceVersion: "3.1",
		Environment:    "test",
		Sampler:        SamplerConfig{Type: "always", Ratio: 0.5},
	}
	o2.FillDefaults()
	if o2.Exporter != "kafka" || o2.ServiceName != "svc-x" || o2.ServiceVersion != "3.1" || o2.Environment != "test" {
		t.Errorf("显式值被覆盖: exporter=%q svc=%q ver=%q env=%q", o2.Exporter, o2.ServiceName, o2.ServiceVersion, o2.Environment)
	}
	if o2.Sampler.Type != "always" || o2.Sampler.Ratio != 0.5 {
		t.Errorf("显式 sampler 被覆盖: type=%q ratio=%v", o2.Sampler.Type, o2.Sampler.Ratio)
	}
}

// TestMQConfigFillDefaultsExplicitPreserved 显式值不被覆盖 + 零值 Timeout/MaxRetries 填充
func TestMQConfigFillDefaultsExplicitPreserved(t *testing.T) {
	mq := MQConfig{
		Type:       "pulsar",
		Brokers:    []string{"pulsar://h:6650"},
		Topic:      "custom-topic",
		GroupID:    "grp-x",
		MaxRetries: 7,
		Timeout:    30 * time.Second,
	}
	mq.FillDefaults()
	if mq.Type != "pulsar" || mq.Topic != "custom-topic" || mq.GroupID != "grp-x" {
		t.Errorf("显式值被覆盖: type=%q topic=%q group=%q", mq.Type, mq.Topic, mq.GroupID)
	}
	if len(mq.Brokers) != 1 || mq.Brokers[0] != "pulsar://h:6650" {
		t.Errorf("Brokers 被覆盖: %v", mq.Brokers)
	}
	if mq.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7（显式保留）", mq.MaxRetries)
	}
	if mq.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want 30s（显式保留）", mq.Timeout)
	}

	// 零值 Timeout/MaxRetries → 默认 5s / 3（TestMQConfigFillDefaults 未覆盖 Timeout 分支）
	var m2 MQConfig
	m2.FillDefaults()
	if m2.MaxRetries != 3 || m2.Timeout != 5*time.Second {
		t.Errorf("零值 MQ = retries %d timeout %v, want 3 / 5s", m2.MaxRetries, m2.Timeout)
	}
}
