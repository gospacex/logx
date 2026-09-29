// logx/example/03-otel-tracing/03_test.go
// 用例 03 测试：OTel 调用链集成（双模式配置 + TracerProvider 装配 + 各级别日志入链）
//
// 零依赖部分（默认运行）：
//
//   - loadConfig 双模式：程序内构造 vs HUBX_CONFIG config.yaml
//   - demoAllLevels 全级别日志入链（exporter=stdout，无外部依赖）
//
// 门控集成部分（需 docker compose 起 Jaeger，HUBX_JAEGER_IT=1 启用）：
//
//   - jaeger exporter 真实推送 + ForceFlush
//
//     运行：HUBX_JAEGER_IT=1 go test -run TestExample03JaegerIT -v .
package main

import (
	"context"
	"os"
	"testing"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（与用例 03 一致）
	zapimpl "github.com/gospacex/logx/zap"
)

// TestExample03InProgramConfig 双模式之一：无 HUBX_CONFIG 时程序内构造 cfg
func TestExample03InProgramConfig(t *testing.T) {
	t.Setenv("HUBX_CONFIG", "")
	cfg := loadConfig()

	if configSource != "in-program" {
		t.Errorf("configSource = %q, want %q", configSource, "in-program")
	}
	if cfg.Level != "debug" {
		t.Errorf("Level = %q, want debug", cfg.Level)
	}
	if cfg.ServiceName != "logx-demo-otel" {
		t.Errorf("ServiceName = %q, want logx-demo-otel", cfg.ServiceName)
	}
	if !cfg.OTel.Enabled || cfg.OTel.Exporter != "jaeger" {
		t.Errorf("OTel.Enabled/Exporter = %v/%q, want true/jaeger", cfg.OTel.Enabled, cfg.OTel.Exporter)
	}
	if cfg.OTel.Jaeger.Endpoint != "http://localhost:24268/api/traces" {
		t.Errorf("Jaeger.Endpoint = %q, want http://localhost:24268/api/traces", cfg.OTel.Jaeger.Endpoint)
	}
	if cfg.OTel.Sampler.Type != "parentbased_traceidratio" || cfg.OTel.Sampler.Ratio != 1.0 {
		t.Errorf("Sampler = %+v, want parentbased_traceidratio/1.0", cfg.OTel.Sampler)
	}
	// FillDefaults 回落：ServiceVersion/Environment 继承顶层
	if cfg.ServiceVersion == "" || cfg.Environment == "" {
		t.Errorf("FillDefaults 未填充: version=%q env=%q", cfg.ServiceVersion, cfg.Environment)
	}
}

// TestExample03YamlConfig 双模式之二：HUBX_CONFIG 指向 config.yaml 时经 ReadConfig 加载
func TestExample03YamlConfig(t *testing.T) {
	if _, err := os.Stat("config.yaml"); err != nil {
		t.Skipf("config.yaml 不存在（cwd=%s）: %v", os.TempDir(), err)
	}
	t.Setenv("HUBX_CONFIG", "config.yaml")
	cfg := loadConfig()

	if configSource != "HUBX_CONFIG config.yaml" {
		t.Errorf("configSource = %q, want %q", configSource, "HUBX_CONFIG config.yaml")
	}
	// 与 config.yaml logx: 段一致
	if cfg.ServiceName != "logx-demo-otel" {
		t.Errorf("yaml ServiceName = %q, want logx-demo-otel", cfg.ServiceName)
	}
	if cfg.Level != "debug" {
		t.Errorf("yaml Level = %q, want debug", cfg.Level)
	}
	if !cfg.OTel.Enabled || cfg.OTel.Exporter != "jaeger" {
		t.Errorf("yaml OTel.Enabled/Exporter = %v/%q, want true/jaeger", cfg.OTel.Enabled, cfg.OTel.Exporter)
	}
	if cfg.OTel.Sampler.Type != "parentbased_traceidratio" {
		t.Errorf("yaml Sampler.Type = %q, want parentbased_traceidratio", cfg.OTel.Sampler.Type)
	}
	// 与程序内构造路径字段等价性：核心差异仅来源标记
	if cfg.Environment != "development" {
		t.Errorf("yaml Environment = %q, want development", cfg.Environment)
	}
}

// TestExample03SpanEventsAllLevels 各级别日志入链（stdout exporter，零外部依赖）
// 复刻 main.go 序列：SetupTracerProvider → demoAllLevels（root + 子 span）→ ForceFlush
func TestExample03SpanEventsAllLevels(t *testing.T) {
	t.Setenv("HUBX_CONFIG", "")
	cfg := loadConfig()
	// 换用 stdout exporter：span 以 JSON dump，无需 Jaeger 即可完整走装配 → 入链 → 刷出通路
	cfg.OTel.Exporter = "stdout"

	logger, err := logx.New(cfg)
	if err != nil {
		t.Fatalf("logx.New: %v", err)
	}
	defer logger.Close()
	logx.SetGlobal(logger)
	defer logx.CloseGlobal()

	setup, err := zapimpl.SetupTracerProvider(cfg.OTel)
	if err != nil {
		t.Fatalf("SetupTracerProvider(stdout): %v", err)
	}
	defer func() { _ = setup.Provider.Shutdown(context.Background()) }()

	demoAllLevels(logger, setup) // 各级别 + 子 span；不应 panic/报错
	if err := setup.Provider.ForceFlush(context.Background()); err != nil {
		t.Errorf("ForceFlush: %v", err)
	}
}

// TestExample03JaegerIT 门控集成：jaeger exporter 真实推送（需 docker compose + HUBX_JAEGER_IT=1）
func TestExample03JaegerIT(t *testing.T) {
	if os.Getenv("HUBX_JAEGER_IT") == "" {
		t.Skip("set HUBX_JAEGER_IT=1 (docker compose up -d) to enable Jaeger integration test")
	}
	t.Setenv("HUBX_CONFIG", "")
	cfg := loadConfig() // 默认 jaeger endpoint http://localhost:24268/api/traces
	if ep := os.Getenv("LOGX_JAEGER_ENDPOINT"); ep != "" {
		cfg.OTel.Jaeger.Endpoint = ep
	}

	logger, err := logx.New(cfg)
	if err != nil {
		t.Fatalf("logx.New: %v", err)
	}
	defer logger.Close()

	setup, err := zapimpl.SetupTracerProvider(cfg.OTel)
	if err != nil {
		t.Fatalf("SetupTracerProvider(jaeger): %v", err)
	}
	defer func() { _ = setup.Provider.Shutdown(context.Background()) }()

	demoAllLevels(logger, setup)
	if err := setup.Provider.ForceFlush(context.Background()); err != nil {
		t.Errorf("ForceFlush 到 jaeger: %v", err)
	}
	t.Log("spans pushed; 验证: curl -s localhost:26686/api/traces | grep logx-demo-otel（UI v1 检索）")
}
