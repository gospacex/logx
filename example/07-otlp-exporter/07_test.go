// logx/example/07-otlp-exporter/07_test.go
// 用例 07 测试：OTLP HTTP 导出器（与用例 03 对照：otlp 走 OTLP HTTP 端点，
// 验证 OTelConfig.OTLP 配置段契约）
//
// 零依赖部分（默认运行）：
//
//   - OTelConfig 装配契约：Exporter=otlp 时 SetupTracerProvider 成功构造
//   - defaultEnv 环境变量覆盖边界
//   - 缺 OTLP 端点时的 fail fast 配置校验（main.go 的 log.Fatalf 路径，测试以断言替代）
//
// 门控集成（LOGX_OTLP_ENDPOINT 非空时运行）：
//
//   - 真实 OTLP HTTP 推送 + ForceFlush（docker compose Jaeger 24318）
package main

import (
	"context"
	"os"
	"testing"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（与用例 07 一致）
	zapimpl "github.com/gospacex/logx/zap"
	"go.uber.org/zap"
)

// TestExample07OTLPConfigContract OTelConfig.OTLP 配置段契约：
// Exporter=otlp + OTLP.Endpoint 完整 URL 可成功装配 TracerProvider
func TestExample07OTLPConfigContract(t *testing.T) {
	cfg := logx.DefaultConfig()
	cfg.Level = "info"
	cfg.ServiceName = serviceName
	cfg.OTel.Enabled = true
	cfg.OTel.Exporter = "otlp"
	cfg.OTel.OTLP.Endpoint = "http://localhost:4318" // 完整 URL 走 WithEndpointURL + Insecure
	cfg.FillDefaults()

	// main.go 的 fail fast 校验：缺端点时 log.Fatalf——此处等价断言
	if cfg.OTel.OTLP.Endpoint == "" {
		t.Fatal("OTLP endpoint 未配置（logx.otel.otlp.endpoint）")
	}

	// 装配成功（构造期不校验端点可达性，真实推送在集成测试）
	setup, err := zapimpl.SetupTracerProvider(cfg.OTel)
	if err != nil {
		t.Fatalf("SetupTracerProvider(otlp): %v", err)
	}
	_ = setup.Provider.Shutdown(context.Background())
}

// TestExample07DefaultEnv 环境变量覆盖边界（main.go 的 defaultEnv 契约）
func TestExample07DefaultEnv(t *testing.T) {
	// 未设置 → 默认值
	t.Setenv("LOGX_OTLP_ENDPOINT_TEST", "")
	if got := defaultEnv("LOGX_OTLP_ENDPOINT_TEST", "http://localhost:24318"); got != "http://localhost:24318" {
		t.Errorf("defaultEnv(空) = %q, want 默认值", got)
	}
	// 已设置 → 覆盖
	t.Setenv("LOGX_OTLP_ENDPOINT_TEST", "http://custom:4318")
	if got := defaultEnv("LOGX_OTLP_ENDPOINT_TEST", "http://localhost:24318"); got != "http://custom:4318" {
		t.Errorf("defaultEnv(已设置) = %q, want http://custom:4318", got)
	}
}

// TestExample07OTLPPushIT 门控集成：真实 OTLP HTTP 端点推送 span
// （LOGX_OTLP_ENDPOINT 未设置 → skip；与用例 03 的 HUBX_JAEGER_IT 门控对称）
func TestExample07OTLPPushIT(t *testing.T) {
	endpoint := os.Getenv("LOGX_OTLP_ENDPOINT")
	if endpoint == "" {
		t.Skip("set LOGX_OTLP_ENDPOINT (docker compose up -d, Jaeger OTLP HTTP 24318) to enable OTLP integration test")
	}

	cfg := logx.DefaultConfig()
	cfg.Level = "info"
	cfg.ServiceName = serviceName
	cfg.OTel.Enabled = true
	cfg.OTel.Exporter = "otlp"
	cfg.OTel.OTLP.Endpoint = endpoint
	cfg.FillDefaults()

	logger, err := logx.New(cfg)
	if err != nil {
		t.Fatalf("logx.New: %v", err)
	}
	defer logger.Close()

	setup, err := zapimpl.SetupTracerProvider(cfg.OTel)
	if err != nil {
		t.Fatalf("SetupTracerProvider(otlp): %v", err)
	}
	defer func() { _ = setup.Provider.Shutdown(context.Background()) }()

	logger.Info("tracer provider ready (otlp exporter)",
		zap.String("otlp.endpoint", cfg.OTel.OTLP.Endpoint),
	)

	// main.go 的 span 序列：Info + Warn 入链
	ctx, span := setup.Provider.Tracer(serviceName).Start(context.Background(), "otlp-span")
	logger.InfoContext(ctx, "order placed", zap.String("order_id", "1001"))
	logger.WarnContext(ctx, "slow query")
	span.End()

	// 真实推送（端点可达时成功；不可达时 ForceFlush 报 exporter 错误 → 测试失败以暴露环境）
	if err := setup.Provider.ForceFlush(context.Background()); err != nil {
		t.Errorf("ForceFlush 到 OTLP HTTP (%s): %v", endpoint, err)
	}
}
