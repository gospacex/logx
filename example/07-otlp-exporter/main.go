// logx/example/07-otlp-exporter/main.go
// 用例 07：OTLP HTTP 导出器（与 03 对照：exporter=jaeger 走 Thrift collector，
// 本用例 exporter=otlp 走 OTLP HTTP 端点，验证 OTelConfig.OTLP 配置段）
//
// 前置条件：
//
//	cd logx/example && docker compose up -d   # 仅需要 Jaeger (OTLP HTTP 24318)
//
// 运行：
//
//	go run .
//	LOGX_OTLP_ENDPOINT=http://host:4318 go run .   # 覆盖 OTLP 端点
//
// 覆盖功能矩阵：
//
//	§3 OTel 导出目标：OTelConfig.OTLP.Endpoint（Exporter=otlp 时生效）+ TracerProvider 装配
//	§4 span → OTLP HTTP → Jaeger 全链路（与 jaeger exporter 的 Thrift 通路互为对照）
//
// 验证：Jaeger UI http://localhost:26686 搜索服务 logx-demo-otlp
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（空白导入）
	zapimpl "github.com/gospacex/logx/zap"
	"go.uber.org/zap"
)

const serviceName = "logx-demo-otlp"

func main() {
	cfg := logx.DefaultConfig()
	cfg.Level = "info"
	cfg.ServiceName = serviceName
	cfg.OTel.Enabled = true
	cfg.OTel.Exporter = "otlp" // 走 OTLP HTTP 端点（非 jaeger 默认 Thrift 通路）
	cfg.OTel.OTLP.Endpoint = defaultEnv("LOGX_OTLP_ENDPOINT", "http://localhost:24318")
	cfg.FillDefaults()

	logger, err := logx.New(cfg)
	if err != nil {
		log.Fatalf("logx.New failed: %v", err)
	}
	defer logger.Close()

	// 缺 OTLP 端点时 fail fast（装配期报错，与 kafka 缺 brokers 同契约）
	// 此处直接校验配置，避免静默落到缺省端点
	if cfg.OTel.OTLP.Endpoint == "" {
		log.Fatalf("otlp endpoint 未配置（logx.otel.otlp.endpoint）")
	}

	setup, err := zapimpl.SetupTracerProvider(cfg.OTel)
	if err != nil {
		log.Fatalf("SetupTracerProvider failed: %v", err)
	}
	defer func() { _ = setup.Provider.Shutdown(context.Background()) }()

	logger.Info("tracer provider ready (otlp exporter)",
		zap.String("otlp.endpoint", cfg.OTel.OTLP.Endpoint),
	)

	// span 内打 3 条日志（入链为 Span Event，随 span 经 OTLP HTTP 推送）
	ctx, span := setup.Provider.Tracer(serviceName).Start(context.Background(), "otlp-span")
	logger.InfoContext(ctx, "order placed",
		zap.String("order_id", "1001"),
		zap.String("db.system", "mysql"),
	)
	time.Sleep(20 * time.Millisecond)
	logger.WarnContext(ctx, "slow query", zap.Duration("db.latency", 800*time.Millisecond))
	span.End()
	_ = setup.Provider.ForceFlush(context.Background())

	logger.Info("span pushed via OTLP HTTP",
		zap.String("verify", "open http://localhost:26686 and search service "+serviceName),
	)
}

// defaultEnv 读环境变量，为空返回默认值
func defaultEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
