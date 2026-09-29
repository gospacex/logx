// logx/config_test.go
// logx 配置单元测试
package logx

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Level != "info" {
		t.Errorf("default level = %q, want %q", cfg.Level, "info")
	}
	if cfg.Format != "console" {
		t.Errorf("default format = %q, want %q", cfg.Format, "console")
	}
	if cfg.Output != "stdout" {
		t.Errorf("default output = %q, want %q", cfg.Output, "stdout")
	}
	if cfg.OTel.Enabled {
		t.Error("default otel should be disabled")
	}
	if cfg.OTel.Exporter != "jaeger" {
		t.Errorf("default otel exporter = %q, want %q", cfg.OTel.Exporter, "jaeger")
	}
}

func TestFillDefaults(t *testing.T) {
	cfg := Config{}
	cfg.FillDefaults()
	if cfg.Level != "info" {
		t.Errorf("filled level = %q, want info", cfg.Level)
	}
	if cfg.MaxSize != 100 {
		t.Errorf("filled max_size = %d, want 100", cfg.MaxSize)
	}
	if cfg.OTel.ServiceName != "logx" {
		t.Errorf("filled otel service name = %q, want logx", cfg.OTel.ServiceName)
	}
	if cfg.MQ.Type != "" {
		t.Errorf("mq type should stay empty until set, got %q", cfg.MQ.Type)
	}
}

// TestConfigYAML 验证 config.yaml 的 logx 字段可以正确解析
func TestConfigYAML(t *testing.T) {
	yamlDoc := `
app:
  name: myapp
logx:
  level: debug
  format: console
  output: stdout
  file_path: ./logs/app.log
  max_size: 50
  max_backups: 3
  max_age: 7
  compress: false
  otel:
    enabled: true
    exporter: jaeger
    jaeger:
      endpoint: "http://localhost:14268/api/traces"
`

	var doc struct {
		Logx Config `yaml:"logx"`
	}
	if err := yaml.Unmarshal([]byte(yamlDoc), &doc); err != nil {
		t.Fatalf("unmarshal yaml: %v", err)
	}
	cfg := doc.Logx
	if cfg.Level != "debug" {
		t.Errorf("level = %q, want debug", cfg.Level)
	}
	if !cfg.OTel.Enabled {
		t.Error("otel.enabled should be true")
	}
	if cfg.OTel.Jaeger.Endpoint != "http://localhost:14268/api/traces" {
		t.Errorf("jaeger endpoint = %q", cfg.OTel.Jaeger.Endpoint)
	}
	if cfg.MaxSize != 50 {
		t.Errorf("max_size = %d, want 50", cfg.MaxSize)
	}
}

func TestMQConfigFillDefaults(t *testing.T) {
	mq := MQConfig{Type: "kafka", Brokers: []string{"localhost:9092"}}
	mq.FillDefaults()
	if mq.Topic != "otel-traces" {
		t.Errorf("default topic = %q, want otel-traces", mq.Topic)
	}
	if mq.GroupID != "logx-otel-consumer" {
		t.Errorf("default group = %q", mq.GroupID)
	}
	if mq.MaxRetries != 3 {
		t.Errorf("default max_retries = %d, want 3", mq.MaxRetries)
	}
}

func TestFillDefaults_ServiceDefaults(t *testing.T) {
	cfg := logxDefaultCfgForTest()
	cfg.DefaultService = "svc-a"
	cfg.FillDefaults()
	if cfg.DefaultService != "svc-a" {
		t.Errorf("DefaultService = %q, want svc-a", cfg.DefaultService)
	}

	empty := Config{}
	empty.FillDefaults()
	if empty.DefaultService != "" || len(empty.DefaultTags) != 0 {
		t.Errorf("zero config defaults should stay empty, got svc=%q tags=%v",
			empty.DefaultService, empty.DefaultTags)
	}
}

// logxDefaultCfgForTest 返回最小配置（OTel 关闭，避免装配副作用）
func logxDefaultCfgForTest() Config {
	cfg := DefaultConfig()
	cfg.OTel.Enabled = false
	return cfg
}
