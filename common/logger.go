// logx/common/logger.go
// context 注入/提取请求级 logger
package common

import (
	"context"

	"github.com/gospacex/logx"
)

// loggerKey context key
type loggerKey struct{}

// WithLogger 将 logger 注入 context
// 在 Hertz 中间件中调用：ctx = common.WithLogger(ctx, reqLogger)
func WithLogger(ctx context.Context, l logx.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// GetLogger 从 context 获取 logger
// 若 context 中没有注入 logger，返回全局 Logger（带 source 标注）
func GetLogger(ctx context.Context) logx.Logger {
	if ctx == nil {
		return logx.GetGlobal()
	}
	if l, ok := ctx.Value(loggerKey{}).(logx.Logger); ok && l != nil {
		return l
	}
	return logx.GetGlobal()
}

// TryGetLogger 尝试从 context 获取 logger；未注入时返回（nil, false）
func TryGetLogger(ctx context.Context) (logx.Logger, bool) {
	if ctx == nil {
		return nil, false
	}
	l, ok := ctx.Value(loggerKey{}).(logx.Logger)
	return l, ok && l != nil
}
