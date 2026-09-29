// logx/builder.go
// 链式 Builder：logx.L() 全局入口 / logger.Chain() 实例入口
// 键值交替变参字段（"k", v）由日志后端注册归一化实现（zap 包 init 注册 zap.NormalizedAny）；
// 根包不 import logx/zap，避免循环依赖。
package logx

import (
	"context"
	"strings"

	"go.uber.org/zap/zapcore"
)

// Fields 字典风格字段（key 支持中文；作为 ...any 变参元素时自动展开为键值对）
type Fields map[string]any

// Builder 链式日志构建器：中间方法（Service/Operation/Tags/WithContext）不输出，
// 末端各级别方法（Debug/Info/Warn/Error/DPanic）输出
type Builder struct {
	l         Logger
	service   string
	operation string
	tags      []string
	ctx       context.Context
}

// L 返回全局链式入口；末端输出走当前全局 Logger，
// 未 SetGlobal 时自动创建 stdout Logger 保证可用
func L() *Builder {
	g := GetGlobal()
	if g == nil {
		g = noopLogger{}
	}
	return &Builder{l: g, ctx: context.Background()}
}

// Attach 绑定指定 Logger 实例为本 Builder 的输出目标
// （后端实现 Chain() 时调用；绑定后的中间方法与末端方法均走该实例）
func (b *Builder) Attach(l Logger) *Builder {
	if l != nil {
		b.l = l
	}
	return b
}

// Service 指定服务名（日志字段 log.service；注入 span 属性 service.name）
func (b *Builder) Service(name string) *Builder { b.service = name; return b }

// Operation 指定操作（url 或模块名；日志字段 log.operation；span 属性 operation）
func (b *Builder) Operation(op string) *Builder { b.operation = op; return b }

// Tags 追加标签（与配置 DefaultTags 并集去重；日志字段 log.tags 逗号拼接）
func (b *Builder) Tags(tags ...string) *Builder {
	b.tags = append(b.tags, tags...)
	return b
}

// WithContext 携带上下文（ctx 含活动 recording span 时注入属性，方案 A）
func (b *Builder) WithContext(ctx context.Context) *Builder {
	if ctx != nil {
		b.ctx = ctx
	}
	return b
}

// ===== 末端输出（键值交替 ...any：string 与值成对；末尾落单 string 追加消息）=====

func (b *Builder) Debug(msg string, fields ...any)  { b.emit(zapcore.DebugLevel, msg, fields...) }
func (b *Builder) Info(msg string, fields ...any)   { b.emit(zapcore.InfoLevel, msg, fields...) }
func (b *Builder) Warn(msg string, fields ...any)   { b.emit(zapcore.WarnLevel, msg, fields...) }
func (b *Builder) Error(msg string, fields ...any)  { b.emit(zapcore.ErrorLevel, msg, fields...) }
func (b *Builder) DPanic(msg string, fields ...any) { b.emit(zapcore.DPanicLevel, msg, fields...) }

// emit 末端输出统一入口
func (b *Builder) emit(lvl zapcore.Level, msg string, fields ...any) {
	if b.l == nil {
		return
	}
	flds, extraMsg := NormalizeFields(fields...)
	if extraMsg != "" {
		msg = msg + " " + extraMsg
	}
	svc, op, tags := b.resolveAttrs()
	attr := make([]Field, 0, 4)
	if svc != "" {
		attr = append(attr, String("log.service", svc))
	}
	if op != "" {
		attr = append(attr, String("log.operation", op))
	}
	if len(tags) > 0 {
		attr = append(attr, String("log.tags", strings.Join(tags, ",")))
	}
	// 方案 A：ctx 活动 span 属性注入（无 span 静默降级）
	if inj, ok := b.l.(spanAttrInjctor); ok {
		inj.InjectSpanAttrs(b.ctx, svc, op, tags)
	}
	all := append(attr, flds...)
	switch lvl {
	case zapcore.DebugLevel:
		b.l.DebugContext(b.ctx, msg, all...)
	case zapcore.InfoLevel:
		b.l.InfoContext(b.ctx, msg, all...)
	case zapcore.WarnLevel:
		b.l.WarnContext(b.ctx, msg, all...)
	case zapcore.ErrorLevel:
		b.l.ErrorContext(b.ctx, msg, all...)
	default:
		b.l.DPanicContext(b.ctx, msg, all...)
	}
}

// resolveAttrs 回退链：显式 > Logger 配置默认 > ServiceName 兜底；Tags 并集去重
func (b *Builder) resolveAttrs() (service, operation string, tags []string) {
	if b.l == nil {
		return b.service, b.operation, dedupeTags(b.tags)
	}
	service = b.service
	if service == "" {
		service = b.l.DefaultService()
	}
	if service == "" {
		if sn, ok := b.l.(interface{ ServiceName() string }); ok {
			service = sn.ServiceName()
		}
	}
	operation = b.operation
	if operation == "" {
		operation = b.l.DefaultOperation()
	}
	merged := append([]string{}, b.tags...)
	merged = append(merged, b.l.DefaultTags()...)
	tags = dedupeTags(merged)
	return
}

// dedupeTags 并集去重（保序：链式在前）
func dedupeTags(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// ===== 归一化注入（后端包 init 注册，避免 logx → logx/zap 循环依赖）=====

// NormalizeFields ...any → []Field + 追加消息片段。
// 由日志后端注册实现（zap 包 init 时注册 zap.NormalizedAny）；
// 未注册时退化：每个值以 Any("arg") 包装
var normalizeFieldsFn func(...any) ([]Field, string)

// RegisterFieldNormalizer 注册 ...any 字段归一化实现（后端包 init 调用）
func RegisterFieldNormalizer(fn func(...any) ([]Field, string)) {
	normalizeFieldsFn = fn
}

// NormalizeFields 调用已注册归一化器
func NormalizeFields(vv ...any) ([]Field, string) {
	if normalizeFieldsFn != nil {
		return normalizeFieldsFn(vv...)
	}
	out := make([]Field, 0, len(vv))
	for _, v := range vv {
		out = append(out, Any("arg", v))
	}
	return out, ""
}

// spanAttrInjctor 活动 span 属性注入接口（zap 后端实现；方案 A）
type spanAttrInjctor interface {
	InjectSpanAttrs(ctx context.Context, service, operation string, tags []string)
}
