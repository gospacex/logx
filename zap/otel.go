// logx/zap/otel.go
// OTelCore：zapcore.Core 实现，将日志条目转换为 OTel Span Event
// 开启 OTel 后，所有经过该 Core 的日志（带 context span）会：
//  1. 作为 Span Event 写入当前调用链（包含 trace_id、span_id、timestamp 与日志字段）
//  2. 同时照常输出到下游 Core（stdout/file）
package zap

import (
	"context"
	"math"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"
)

// spanFieldKey OTelCore 识别 span 的隐藏字段 key
const spanFieldKey = "__logx_span"

// spanField 创建一个隐藏的 zap 字段，携带 trace.Span 供 OTelCore 提取。
// 使用 SkipType 保证不会进入任何编码器输出。
func spanField(span trace.Span) zapcore.Field {
	return zapcore.Field{
		Key:       spanFieldKey,
		Type:      zapcore.SkipType,
		Interface: span,
	}
}

// OTelCore 将日志条目转换为 Span Event 的 zapcore.Core
type OTelCore struct {
	// core 下游输出核心（stdout/file）
	core zapcore.Core
	// enabled 是否启用 Span Event 注入
	enabled bool
}

// NewOTelCore 包装一个基础 Core，使其将日志转为 Span Event
func NewOTelCore(core zapcore.Core) *OTelCore {
	return &OTelCore{core: core, enabled: true}
}

// SetEnabled 动态开关 Span Event 注入
func (c *OTelCore) SetEnabled(enabled bool) {
	c.enabled = enabled
}

// Enabled 判断日志级别是否被下游启用
func (c *OTelCore) Enabled(lvl zapcore.Level) bool {
	return c.core.Enabled(lvl)
}

// With 添加结构化字段并返回新的 Core
func (c *OTelCore) With(fields []zapcore.Field) zapcore.Core {
	return &OTelCore{core: c.core.With(fields), enabled: c.enabled}
}

// Check 检查条目
func (c *OTelCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.core.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

// Write 将日志条目写入调用链 Span Event，并转发到下游 Core
func (c *OTelCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	// 1. 提取隐藏的 span 字段，并将剩余字段传给下游
	span, rest := extractSpan(fields)
	if c.enabled && span != nil && span.IsRecording() {
		addSpanEvent(span, ent, rest)
	}
	return c.core.Write(ent, rest)
}

// Sync 同步下游
func (c *OTelCore) Sync() error {
	return c.core.Sync()
}

// ======================= helper =======================

// extractSpan 从字段中提取隐藏 span，返回 (span, 过滤后的字段)
func extractSpan(fields []zapcore.Field) (trace.Span, []zapcore.Field) {
	for i, f := range fields {
		if f.Type == zapcore.SkipType && f.Key == spanFieldKey {
			if span, ok := f.Interface.(trace.Span); ok {
				rest := make([]zapcore.Field, 0, len(fields)-1)
				rest = append(rest, fields[:i]...)
				rest = append(rest, fields[i+1:]...)
				return span, rest
			}
			break
		}
	}
	return nil, fields
}

// addSpanEvent 将日志条目作为 Span Event 写入 span
// 事件包含：message、level、timestamp、以及所有日志字段转换的 OTel 属性
func addSpanEvent(span trace.Span, ent zapcore.Entry, fields []zapcore.Field) {
	attrs := make([]attribute.KeyValue, 0, len(fields)+2)
	attrs = append(attrs,
		attribute.String("log.level", ent.Level.String()),
		attribute.String("log.logger", ent.LoggerName),
	)
	if ent.Caller.Defined {
		attrs = append(attrs, attribute.String("code.filepath", ent.Caller.File),
			attribute.Int("code.lineno", ent.Caller.Line))
	}
	for _, f := range fields {
		if kv, ok := fieldToAttribute(f); ok {
			attrs = append(attrs, kv)
		}
	}

	span.AddEvent(ent.Message,
		trace.WithAttributes(attrs...),
		trace.WithTimestamp(ent.Time),
	)
}

// fieldToAttribute 将 zap 字段转换为 OTel 属性
// 不支持的字段类型返回 (zero, false)
func fieldToAttribute(f zapcore.Field) (attribute.KeyValue, bool) {
	key := attribute.Key(f.Key)
	switch f.Type {
	case zapcore.BoolType:
		return key.Bool(f.Integer == 1), true
	case zapcore.ByteStringType:
		return key.String(string(f.Interface.([]byte))), true
	case zapcore.DurationType:
		return key.Int64(f.Integer), true // 纳秒
	case zapcore.Float64Type:
		return key.Float64(float64FromBits(f.Integer)), true
	case zapcore.Float32Type:
		return key.Float64(float64(float32FromBits(f.Integer))), true
	case zapcore.Int64Type:
		return key.Int64(f.Integer), true
	case zapcore.Int32Type:
		return key.Int64(int64(int32(f.Integer))), true
	case zapcore.Int16Type:
		return key.Int64(int64(int16(f.Integer))), true
	case zapcore.Int8Type:
		return key.Int64(int64(int8(f.Integer))), true
	case zapcore.StringType:
		return key.String(f.String), true
	case zapcore.TimeType:
		return key.String(unixNanoToTime(f.Integer).Format(time.RFC3339Nano)), true
	case zapcore.TimeFullType:
		if t, ok := f.Interface.(time.Time); ok {
			return key.String(t.Format(time.RFC3339Nano)), true
		}
	case zapcore.Uint64Type:
		return key.Int64(int64(f.Integer)), true
	case zapcore.Uint32Type:
		return key.Int64(int64(uint32(f.Integer))), true
	case zapcore.Uint16Type:
		return key.Int64(int64(uint16(f.Integer))), true
	case zapcore.Uint8Type:
		return key.Int64(int64(uint8(f.Integer))), true
	case zapcore.StringerType:
		if s, ok := f.Interface.(interface{ String() string }); ok {
			return key.String(s.String()), true
		}
	case zapcore.ErrorType:
		if err, ok := f.Interface.(error); ok {
			return key.String(err.Error()), true
		}
	case zapcore.ReflectType, zapcore.ObjectMarshalerType, zapcore.InlineMarshalerType:
		if s, ok := f.Interface.(string); ok {
			return key.String(s), true
		}
		// 非字符串的反射字段：使用默认 String() 表示
		if f.Interface != nil {
			return key.String(stringify(f.Interface)), true
		}
	}
	return attribute.KeyValue{}, false
}

// stringify 将任意对象转换为字符串（兜底）
func stringify(v interface{}) string {
	if s, ok := v.(interface{ String() string }); ok {
		return s.String()
	}
	return "nil"
}

// ======================= 位转换 helper =======================
// zap 将 float 编码为 IEEE-754 位模式存入 Field.Integer

func float64FromBits(bits int64) float64  { return math.Float64frombits(uint64(bits)) }
func float32FromBits(bits int64) float32  { return math.Float32frombits(uint32(bits)) }
func unixNanoToTime(nano int64) time.Time { return time.Unix(0, nano) }

// context 包在 OTelCore 中用于类型标注（保持 import 一致性）
var _ = context.Background
