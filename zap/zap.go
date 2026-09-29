// logx/zap/zap.go
// Zap 实现：统一日志接口的 Zap 后端
// 支持 console/json 格式、stdout/file/both 输出、OTel 调用链集成
package zap

import (
	"context"
	"os"
	"sync"

	"github.com/gospacex/logx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

// Logger Zap 实现
type Logger struct {
	zl    *zap.Logger
	zlCtx *zap.Logger // *Context 通路专用：write() 多一层调用帧，caller skip 比 zl 多 1
	cfg   logx.Config
	lvl   zapcore.Level

	mu     sync.Mutex
	closer []func() error
}

// provider logx.Provider 的 Zap 实现
type provider struct{}

func (p provider) Name() string { return "zap" }

// New 实现 logx.Provider.New
func (p provider) New(cfg logx.Config) (logx.Logger, error) {
	return New(cfg)
}

// init 注册 Zap 为默认 logx 提供者
// 只要 blank import github.com/gospacex/logx/zap 即可使 logx.New 可用
func init() {
	logx.RegisterProvider(provider{})
	// 注册 ...any 字段归一化实现（根包 Builder 键值交替变参经此归一化为 []zap.Field）
	logx.RegisterFieldNormalizer(normalizeAnyExport)
}

// normalizeAnyExport 包装 NormalizedAny（签名适配 logx.RegisterFieldNormalizer）
func normalizeAnyExport(vv ...any) ([]logx.Field, string) {
	f, extra := NormalizedAny(vv...)
	return f, extra // []zap.Field 即 []logx.Field（别名）
}

// New 创建日志实例
func New(cfg logx.Config) (logx.Logger, error) {
	cfg.FillDefaults()

	// 1. 构建 zapcore.Core（输出后端）
	core, writers, err := buildCore(cfg)
	if err != nil {
		return nil, err
	}

	// 2. 如果开启 OTel，包装 OTelCore（日志 → Span Event）
	if cfg.OTel.Enabled {
		core = NewOTelCore(core)
	}

	// 3. 构建 zap.Logger
	// zl：非 Context 方法通路（l.zl.Info → zapcore，调用帧 = 1 层）
	// zlCtx：*Context 方法通路（l.write → l.zlCtx.Info → zapcore，调用帧 = 2 层）
	// 两实例共享同一 core，仅 caller skip 深度不同，保证两种通路 caller 归因一致
	build := func(skip int) *zap.Logger {
		opts := []zap.Option{
			zap.AddCallerSkip(skip),
			zap.AddStacktrace(zapcore.ErrorLevel),
		}
		if cfg.Caller {
			opts = append(opts, zap.AddCaller())
		}
		if cfg.Development {
			opts = append(opts, zap.Development())
		}
		return zap.New(core, opts...)
	}
	zl := build(1)
	zlCtx := build(2)

	return &Logger{
		zl:     zl,
		zlCtx:  zlCtx,
		cfg:    cfg,
		lvl:    parseLevel(cfg.Level),
		closer: writers,
	}, nil
}

// buildCore 构建输出核心（stdout / file / both）
func buildCore(cfg logx.Config) (zapcore.Core, []func() error, error) {
	level := parseLevel(cfg.Level)

	var enc zapcore.Encoder
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encCfg.EncodeLevel = zapcore.CapitalLevelEncoder
	if cfg.Format == "json" {
		enc = zapcore.NewJSONEncoder(encCfg)
	} else {
		encCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encCfg.EncodeCaller = zapcore.ShortCallerEncoder
		enc = zapcore.NewConsoleEncoder(encCfg)
	}

	var (
		writers []func() error
		cores   []zapcore.Core
	)

	if cfg.Output == "stdout" || cfg.Output == "both" || cfg.Output == "" {
		cores = append(cores, zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), level))
	}

	if cfg.Output == "file" || cfg.Output == "both" {
		lj := &lumberjack.Logger{
			Filename:   cfg.FilePath,
			MaxSize:    cfg.MaxSize,    // MB
			MaxBackups: cfg.MaxBackups, // 个数
			MaxAge:     cfg.MaxAge,     // 天数
			Compress:   cfg.Compress,
		}
		cores = append(cores, zapcore.NewCore(enc, zapcore.AddSync(lj), level))
		writers = append(writers, func() error { return lj.Close() })
	}

	var core zapcore.Core
	switch len(cores) {
	case 0:
		core = zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), level)
	case 1:
		core = cores[0]
	default:
		core = zapcore.NewTee(cores...)
	}

	return core, writers, nil
}

// parseLevel 解析日志级别字符串
func parseLevel(s string) zapcore.Level {
	switch s {
	case "debug":
		return zapcore.DebugLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "dpanic":
		return zapcore.DPanicLevel
	case "panic":
		return zapcore.PanicLevel
	case "fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

// ======================= 基本日志方法 =======================

func (l *Logger) Debug(msg string, fields ...logx.Field) { l.zl.Debug(msg, fields...) }
func (l *Logger) Info(msg string, fields ...logx.Field)  { l.zl.Info(msg, fields...) }
func (l *Logger) Warn(msg string, fields ...logx.Field)  { l.zl.Warn(msg, fields...) }
func (l *Logger) Error(msg string, fields ...logx.Field) { l.zl.Error(msg, fields...) }
func (l *Logger) DPanic(msg string, fields ...logx.Field) {
	l.zl.DPanic(msg, fields...)
}
func (l *Logger) Panic(msg string, fields ...logx.Field) { l.zl.Panic(msg, fields...) }
func (l *Logger) Fatal(msg string, fields ...logx.Field) { l.zl.Fatal(msg, fields...) }

// ======================= Context 日志方法 =======================
// 开启 OTel 时：日志作为 Span Event 写入调用链，并附加 trace_id/span_id
// 关闭 OTel 时：仅输出到 Zap，不注入调用链

func (l *Logger) DebugContext(ctx context.Context, msg string, fields ...logx.Field) {
	l.write(ctx, zapcore.DebugLevel, msg, fields...)
}

func (l *Logger) InfoContext(ctx context.Context, msg string, fields ...logx.Field) {
	l.write(ctx, zapcore.InfoLevel, msg, fields...)
}

func (l *Logger) WarnContext(ctx context.Context, msg string, fields ...logx.Field) {
	l.write(ctx, zapcore.WarnLevel, msg, fields...)
}

func (l *Logger) ErrorContext(ctx context.Context, msg string, fields ...logx.Field) {
	l.write(ctx, zapcore.ErrorLevel, msg, fields...)
}

func (l *Logger) DPanicContext(ctx context.Context, msg string, fields ...logx.Field) {
	l.write(ctx, zapcore.DPanicLevel, msg, fields...)
}

func (l *Logger) PanicContext(ctx context.Context, msg string, fields ...logx.Field) {
	l.write(ctx, zapcore.PanicLevel, msg, fields...)
}

func (l *Logger) FatalContext(ctx context.Context, msg string, fields ...logx.Field) {
	l.write(ctx, zapcore.FatalLevel, msg, fields...)
}

// write 统一 Context 日志写入入口
//   - OTel 开启：插入隐藏的 span 字段，OTelCore 会将其转换为 Span Event
//   - 始终附加 trace_id/span_id 字段（context 有效时）
//   - 经 zlCtx（caller skip = 2）写入：write() 相比非 Context 方法多一层调用帧，
//     保证 caller 归因与 Info/Debug 等非 Context 通路一致（指向用户调用处）
func (l *Logger) write(ctx context.Context, lvl zapcore.Level, msg string, fields ...logx.Field) {
	if !l.zl.Core().Enabled(lvl) {
		return
	}

	merged := make([]logx.Field, 0, len(fields)+2)
	if l.cfg.OTel.Enabled {
		if span := trace.SpanFromContext(ctx); span.IsRecording() {
			// 隐藏字段：OTelCore 识别并转为 Span Event，编码器会跳过（SkipType）
			merged = append(merged, spanField(span))
		}
	}
	merged = append(merged, traceFields(ctx, l.cfg.OTel.Enabled)...)
	merged = append(merged, fields...)

	// zlCtx 可能为 nil（旧派生实例 / 测试构造）：回退 zl（保持可用，caller 归因降级）
	zl := l.zlCtx
	if zl == nil {
		zl = l.zl
	}
	switch lvl {
	case zapcore.DebugLevel:
		zl.Debug(msg, merged...)
	case zapcore.InfoLevel:
		zl.Info(msg, merged...)
	case zapcore.WarnLevel:
		zl.Warn(msg, merged...)
	case zapcore.ErrorLevel:
		zl.Error(msg, merged...)
	case zapcore.DPanicLevel:
		zl.DPanic(msg, merged...)
	case zapcore.PanicLevel:
		zl.Panic(msg, merged...)
	case zapcore.FatalLevel:
		zl.Fatal(msg, merged...)
	default:
		zl.Info(msg, merged...)
	}
}

// traceFields 从 context 提取 trace_id/span_id 字段
// OTel 未开启时不附加任何调用链字段
func traceFields(ctx context.Context, otelEnabled bool) []logx.Field {
	if !otelEnabled || ctx == nil {
		return nil
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	fields := make([]logx.Field, 0, 3)
	fields = append(fields, zap.String("trace_id", sc.TraceID().String()))
	fields = append(fields, zap.String("span_id", sc.SpanID().String()))
	if sc.IsRemote() {
		fields = append(fields, zap.Bool("remote", true))
	}
	return fields
}

// ======================= 派生 Logger =======================

func (l *Logger) With(fields ...logx.Field) logx.Logger {
	der := &Logger{zl: l.zl.With(fields...), cfg: l.cfg, lvl: l.lvl, closer: l.closer}
	if l.zlCtx != nil {
		der.zlCtx = l.zlCtx.With(fields...)
	}
	return der
}

func (l *Logger) Named(name string) logx.Logger {
	der := &Logger{zl: l.zl.Named(name), cfg: l.cfg, lvl: l.lvl, closer: l.closer}
	if l.zlCtx != nil {
		der.zlCtx = l.zlCtx.Named(name)
	}
	return der
}

func (l *Logger) Level() zapcore.Level { return l.lvl }

func (l *Logger) Sync() error { return l.zl.Sync() }

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for _, closeFn := range l.closer {
		if err := closeFn(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	l.closer = nil
	if err := l.zl.Sync(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (l *Logger) Zap() *zap.Logger { return l.zl }

// Chain 实例级链式入口
func (l *Logger) Chain() *logx.Builder {
	return logx.L().Attach(l)
}

// DefaultService/DefaultOperation/DefaultTags/ServiceName：Builder 回退链
func (l *Logger) DefaultService() string   { return l.cfg.DefaultService }
func (l *Logger) DefaultOperation() string { return l.cfg.DefaultOperation }
func (l *Logger) DefaultTags() []string    { return l.cfg.DefaultTags }
func (l *Logger) ServiceName() string      { return l.cfg.ServiceName }

// InjectSpanAttrs 方案 A：ctx 活动 recording span SetAttributes（无 span 静默跳过）
func (l *Logger) InjectSpanAttrs(ctx context.Context, service, operation string, tags []string) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	var attrs []attribute.KeyValue
	if service != "" {
		attrs = append(attrs, attribute.String("service.name", service))
	}
	if operation != "" {
		attrs = append(attrs, attribute.String("operation", operation))
	}
	if len(tags) > 0 {
		attrs = append(attrs, attribute.StringSlice("log.tags", tags))
	}
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
}

// 编译期检查：保证实现 logx.Logger 接口
var _ logx.Logger = (*Logger)(nil)
