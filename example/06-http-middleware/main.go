// logx/example/06-http-middleware/main.go
// 用例 06：Hertz 请求级日志中间件（功能矩阵 §6）
//
// 演示：
//  1. logx/middleware 中间件：请求 id（X-Request-ID）+ 请求级 logger 注入 + 访问日志
//  2. logx/common.GetLogger(ctx) 提取请求级 logger（logger 名 = request_id）
//  3. 请求内 OTel span（http-handler），日志随 span 入链导出到 Jaeger
//  4. 慢请求（duration > 500ms）访问日志自动升级为 WARN + http.slow 字段
//
// 运行：
//
//	cd logx/example/06-http-middleware && go run .
//	LOGX_DEMO_PORT=8081 go run .                         # 覆盖端口
//	LOGX_JAEGER_ENDPOINT=http://host:24268/api/traces go run .
//
// 验证（Jaeger 经 example/docker-compose.yml 启动，UI 26686）：
//
//	curl -i http://127.0.0.1:8080/fast   # 响应头含 X-Request-ID，终端访问日志 INFO
//	curl -i http://127.0.0.1:8080/slow   # 慢请求：终端访问日志 WARN + http.slow
//	日志行 logger 字段 = 对应 request_id
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/gospacex/logx"
	"github.com/gospacex/logx/common"
	logxhertz "github.com/gospacex/logx/middleware"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider（空白导入）
	zapimpl "github.com/gospacex/logx/zap"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultAddr           = "127.0.0.1:8080"
	defaultJaeger         = "http://localhost:24268/api/traces"
	slowThreshold         = 500 * time.Millisecond
	slowSleep             = 800 * time.Millisecond
	tracerInstrumentation = "logx-demo-http"
)

func main() {
	// ===== 1. 初始化日志（程序内构造 cfg；yaml 备选见 config.yaml / HUBX_CONFIG）=====
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Format = "console"
	cfg.Output = "stdout"
	cfg.ServiceName = "logx-demo-http"
	cfg.OTel.Enabled = true
	cfg.OTel.Exporter = "jaeger"
	cfg.OTel.Jaeger.Endpoint = envOr("LOGX_JAEGER_ENDPOINT", defaultJaeger)
	cfg.FillDefaults()

	logger, err := logx.New(cfg)
	if err != nil {
		log.Fatalf("logx.New failed: %v", err)
	}
	defer logger.Close()
	logx.SetGlobal(logger)

	// ===== 2. TracerProvider 装配（优雅降级：装配失败仅提示，日志功能保持可用）=====
	var tracer trace.Tracer
	if setup, err := zapimpl.SetupTracerProvider(cfg.OTel); err != nil {
		fmt.Fprintf(os.Stderr, "TracerProvider setup failed (jaeger up? %s); falling back to no-op tracer, logging stays active: %v\n",
			cfg.OTel.Jaeger.Endpoint, err)
		tracer = otel.Tracer(tracerInstrumentation) // 未装配 → 全局 no-op
	} else {
		defer func() { _ = setup.Provider.Shutdown(context.Background()) }()
		tracer = setup.Provider.Tracer(cfg.ServiceName)
	}

	// ===== 3. Hertz 服务 + logx 中间件 =====
	addr := envOr("LOGX_DEMO_PORT", defaultAddr)
	h := server.New(server.WithHostPorts(addr))
	h.Use(logxhertz.Middleware(&logxhertz.MiddlewareConfig{
		Logger:        logger,
		SlowThreshold: slowThreshold,
	}))

	h.GET("/fast", handler(tracer, "fast"))
	h.GET("/slow", handler(tracer, "slow"))

	fmt.Printf("listening on http://%s\n", addr)
	fmt.Printf("  curl -i http://%s/fast   # 快速请求：访问日志 INFO\n", addr)
	fmt.Printf("  curl -i http://%s/slow   # 慢请求(800ms>500ms)：访问日志 WARN + http.slow\n", addr)
	// 常驻运行；装配成功时 span 经 BatchSpanProcessor 后台导出
	if err := h.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// handler 构造 Hertz handler：慢路由先 sleep 触发慢请求阈值，随后统一执行
// 请求内 OTel span + 请求级 logger（中间件经 c.Next(ctx) 注入 ctx，
// handler 首参 ctx 即携带请求级 logger 的 context）
func handler(tracer trace.Tracer, kind string) func(ctx context.Context, c *app.RequestContext) {
	return func(ctx context.Context, c *app.RequestContext) {
		if kind == "slow" {
			time.Sleep(slowSleep)
		}

		spanCtx, span := tracer.Start(ctx, "http-handler")
		defer span.End()

		reqLogger := common.GetLogger(spanCtx)
		reqLogger.InfoContext(spanCtx, kind+" handler done",
			logx.String("request_id", logxhertz.GetRequestID(c)),
		)
		c.String(consts.StatusOK, "ok")
	}
}

// envOr 读取环境变量，空值返回默认
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
