// logx/middleware/middleware.go
// Hertz 请求级日志中间件：request id + 请求级 logger 注入 + 访问日志（OTel 语义字段）。
// 零 otel 依赖，与 common 包定位一致；OTel 字段名仅为语义约定，不引入 otel SDK。
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gospacex/logx"
	"github.com/gospacex/logx/common"
)

const (
	// requestIDKey hertz RequestContext 存储键（未导出，外部请通过 GetRequestID 获取）。
	// hertz 的 c.Set/Value 仅支持 string 键，故键为内部字符串常量而非 struct 类型。
	requestIDKey = "logx.request.id"
	// headerRequestID 请求 id 回写响应头
	headerRequestID = "X-Request-ID"

	msgVisitLog = "http request"
	msgSlow     = "slow http request"
)

// MiddlewareConfig 配置 Hertz 日志中间件
type MiddlewareConfig struct {
	// Logger 请求级 logger 底座；nil 时按请求回退 logx.GetGlobal()
	Logger logx.Logger
	// SlowThreshold 慢请求阈值；<=0 表示不标记慢请求
	SlowThreshold time.Duration
	// DisableVisitLog 关闭访问日志；缺省（false）输出访问日志，
	// 与 README 契约一致（不传该字段即记录访问日志与慢请求）
	DisableVisitLog bool
}

// Middleware 创建 Hertz 请求级日志中间件：
//  1. 生成请求 id（crypto/rand 8 字节 hex），存入 c.Set 并回写 X-Request-ID 响应头
//  2. 通过 common.WithLogger 将 Named(reqID) 的 logger 注入 ctx（hertz 经 c.Next(ctx) 传递给 handler）
//  3. 请求结束后输出访问日志；duration 超过 SlowThreshold（>0）时升级为 Warn 并附加 http.slow 字段
func Middleware(cfg *MiddlewareConfig) func(ctx context.Context, c *app.RequestContext) {
	threshold := cfgSlowThreshold(cfg)
	emitVisitLog := cfgEmitVisitLog(cfg)

	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		reqID := newRequestID()
		c.Set(requestIDKey, reqID)
		c.Response.Header.Set(headerRequestID, reqID)

		logger := cfgBaseLogger(cfg)
		reqLogger := logger.Named(reqID)
		ctx = common.WithLogger(ctx, reqLogger)

		defer func() {
			if !emitVisitLog {
				return
			}
			duration := time.Since(start)
			fields := []logx.Field{
				logx.String("http.method", string(c.Method())),
				logx.String("http.route", string(c.Path())),
				logx.String("http.request.id", reqID),
				logx.Int("http.response.status_code", c.Response.StatusCode()),
				logx.Any("http.response.duration", duration.String()),
			}
			if threshold > 0 && duration > threshold {
				fields = append(fields, logx.Any("http.slow", true))
				reqLogger.WarnContext(ctx, msgSlow, fields...)
				return
			}
			reqLogger.InfoContext(ctx, msgVisitLog, fields...)
		}()

		c.Next(ctx)
	}
}

// GetRequestID 获取中间件写入的当前请求 id；未设置（如未挂中间件）时返回 ""
func GetRequestID(c *app.RequestContext) string {
	if c == nil {
		return ""
	}
	if v, ok := c.Get(requestIDKey); ok {
		id, _ := v.(string)
		return id
	}
	return ""
}

// newRequestID 生成 16 字符 hex 请求 id（crypto/rand 8 字节）
func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 随机源不可用（极端场景）时退化为纳秒时间戳，避免空 id
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// cfgBaseLogger 解析底座 logger；nil 时回退全局 logger（按请求解析，避免捕获构造期全局状态）
func cfgBaseLogger(cfg *MiddlewareConfig) logx.Logger {
	if cfg != nil && cfg.Logger != nil {
		return cfg.Logger
	}
	return logx.GetGlobal()
}

// cfgSlowThreshold 解析慢请求阈值（nil 配置为 0）
func cfgSlowThreshold(cfg *MiddlewareConfig) time.Duration {
	if cfg == nil {
		return 0
	}
	return cfg.SlowThreshold
}

// cfgEmitVisitLog 解析访问日志开关：缺省（含 nil 配置）输出，DisableVisitLog=true 关闭
func cfgEmitVisitLog(cfg *MiddlewareConfig) bool {
	if cfg != nil {
		return !cfg.DisableVisitLog
	}
	return true
}
