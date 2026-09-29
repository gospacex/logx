// logx/logx.go
// logx：所有 x 系列中间件的统一日志入口，基于 Zap + OTel 实现，支持调用链集成。
package logx

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/gospacex/configx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Field 日志字段（zap.Field 别名，保持与 zap 生态兼容）
type Field = zap.Field

// String 构造一个 string 类型的日志字段（避免调用方直接依赖 zap）
func String(key string, val string) Field { return zap.String(key, val) }

// Int 构造一个 int 类型的日志字段
func Int(key string, val int) Field { return zap.Int(key, val) }

// Any 构造一个 interface{} 类型的日志字段
func Any(key string, val interface{}) Field { return zap.Any(key, val) }

// Err 构造一个 error 类型的日志字段
func Err(err error) Field { return zap.Error(err) }

// Logger 统一日志接口
type Logger interface {
	// Debug 输出 debug 级别日志
	Debug(msg string, fields ...Field)
	// Info 输出 info 级别日志
	Info(msg string, fields ...Field)
	// Warn 输出 warn 级别日志
	Warn(msg string, fields ...Field)
	// Error 输出 error 级别日志
	Error(msg string, fields ...Field)
	// DPanic 在开发模式下 panic，生产模式输出 error
	DPanic(msg string, fields ...Field)
	// Panic 输出 panic 级别日志并 panic
	Panic(msg string, fields ...Field)
	// Fatal 输出 fatal 级别日志并退出
	Fatal(msg string, fields ...Field)

	// DebugContext 输出带 context 的 debug 日志（context 含 span 时注入调用链）
	DebugContext(ctx context.Context, msg string, fields ...Field)
	// InfoContext 输出带 context 的 info 日志（context 含 span 时注入调用链）
	InfoContext(ctx context.Context, msg string, fields ...Field)
	// WarnContext 输出带 context 的 warn 日志（context 含 span 时注入调用链）
	WarnContext(ctx context.Context, msg string, fields ...Field)
	// ErrorContext 输出带 context 的 error 日志（context 含 span 时注入调用链）
	ErrorContext(ctx context.Context, msg string, fields ...Field)
	// DPanicContext 输出带 context 的 dpanic 日志（context 含 span 时注入调用链）
	DPanicContext(ctx context.Context, msg string, fields ...Field)
	// PanicContext 输出带 context 的 panic 日志（context 含 span 时注入调用链）并 panic
	PanicContext(ctx context.Context, msg string, fields ...Field)
	// FatalContext 输出带 context 的 fatal 日志（context 含 span 时注入调用链）并退出
	FatalContext(ctx context.Context, msg string, fields ...Field)

	// With 返回带固定字段的派生 Logger
	With(fields ...Field) Logger
	// Named 返回带名称的派生 Logger
	Named(name string) Logger
	// Level 返回当前日志级别
	Level() zapcore.Level
	// Sync 刷新缓冲区
	Sync() error
	// Close 关闭并释放资源（刷新缓冲区、关闭文件、关闭 TracerProvider）
	Close() error
	// Zap 返回底层 *zap.Logger（供高级用法）
	Zap() *zap.Logger

	// Chain 返回绑定本实例的链式 Builder（键值交替变参字段，无需 zap 构造函数）
	Chain() *Builder
	// DefaultService 配置缺省 Service（Builder 回退链）
	DefaultService() string
	// DefaultOperation 配置缺省 Operation（Builder 回退链）
	DefaultOperation() string
	// DefaultTags 配置缺省 Tags（Builder 回退链）
	DefaultTags() []string
	// ServiceName 顶层服务名（Builder 回退链末位兜底）
	ServiceName() string
	// InjectSpanAttrs 活动 span 属性注入（ctx 含 recording span 时 SetAttributes；无 span 静默降级）
	InjectSpanAttrs(ctx context.Context, service, operation string, tags []string)
}

// ======================= Provider 注册机制 =======================
// logx 根包不依赖具体实现；具体实现（如 logx/zap）通过 RegisterProvider 注册。
// 这避免了 logx → logx/zap → logx 的包循环依赖。

// Provider 日志实现提供者
type Provider interface {
	// Name 实现名称（如 "zap"）
	Name() string
	// New 创建 Logger
	New(cfg Config) (Logger, error)
}

var (
	providersMu sync.RWMutex
	providers   = map[string]Provider{}
	// defaultProvider 默认提供者（由实现包在 init 中注册）
	defaultProvider string
)

// RegisterProvider 注册日志提供者
func RegisterProvider(p Provider) {
	providersMu.Lock()
	defer providersMu.Unlock()
	providers[p.Name()] = p
	if defaultProvider == "" {
		defaultProvider = p.Name()
	}
}

// SetDefaultProvider 设置默认提供者
func SetDefaultProvider(name string) error {
	providersMu.Lock()
	defer providersMu.Unlock()
	if _, ok := providers[name]; !ok {
		return fmt.Errorf("logx: provider %q not registered", name)
	}
	defaultProvider = name
	return nil
}

// ======================= 全局 Logger =======================

// global 全局 Logger 句柄
var (
	global     Logger
	globalOnce sync.Once
	// globalCfg 全局配置句柄（由 ReadConfig 设置）
	globalCfg   configx.Config
	globalCfgMu sync.RWMutex
)

// ReadConfig 加载统一配置句柄（所有 x 系列 SDK 共享同一配置源）
//
// 优先级：显式 ConfigOptions > HUBX_CONFIG 环境变量 > ./config.yaml
// 配置中心（Nacos/Apollo）开关由配置文件中的 configx.remote.enabled 控制。
func ReadConfig(opts ...configx.ConfigOptions) error {
	c, err := configx.ReadConfig(opts...)
	if err != nil {
		return err
	}
	globalCfgMu.Lock()
	globalCfg = c
	globalCfgMu.Unlock()
	return nil
}

// GetConfig 获取全局配置句柄
func GetConfig() configx.Config {
	globalCfgMu.RLock()
	defer globalCfgMu.RUnlock()
	return globalCfg
}

// ReadAndCreate 从 configx 读取 logx 段并创建日志实例。
//
// 这里封装的是官方示例里那套三步手工动作：
//
//	logx.ReadConfig()                          // 解析配置来源
//	cfg := logx.Config{}
//	logx.GetConfig().Unmarshal("logx", &cfg)   // 取 logx 段
//	logx.New(cfg)                              // 建实例
//
// 少了这一步，"ReadConfig 之后还得自己 Unmarshal"就成了所有调用方的
// 重复劳动；logx 也因此是唯一必须手工拼配置的 SDK。
//
// 返回的实例由调用方持有。需要让它成为进程默认日志句柄（logx.GetGlobal()
// 与各 SDK 的缺省 logger）时再调 SetGlobal。若只想解析配置不建实例，用
// ReadConfig + GetConfig。
func ReadAndCreate(opts ...configx.ConfigOptions) (Logger, error) {
	// 复用本包的 ReadConfig：它会同时写入 globalCfg，使随后的 GetConfig
	// 与 Unmarshal 可用，调用方不必再读一次配置。
	if err := ReadConfig(opts...); err != nil {
		return nil, fmt.Errorf("logx: failed to read config: %w", err)
	}

	cfg := Config{}
	if err := GetConfig().Unmarshal("logx", &cfg); err != nil {
		return nil, fmt.Errorf("logx: failed to unmarshal logx section: %w", err)
	}
	return New(cfg)
}

// MustReadAndCreate 类似 ReadAndCreate，失败时退出进程
func MustReadAndCreate(opts ...configx.ConfigOptions) Logger {
	l, err := ReadAndCreate(opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logx: failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	return l
}

// New 创建日志实例
// 支持配置：
//   - level/format/output（stdout/file/both）
//   - OTel 调用链集成（配置 logging.otel.enabled）
//   - OTel 数据推送目标（jaeger/mq/otlp）
//
// 若未调用 ReadConfig，回退使用 DefaultConfig()。
func New(cfg Config) (Logger, error) {
	cfg.FillDefaults()

	providersMu.RLock()
	p := providers[defaultProvider]
	providersMu.RUnlock()

	if p == nil {
		// 没有注册任何 provider：需要导入 logx/zap
		return nil, fmt.Errorf("logx: no logger provider registered; import github.com/gospacex/logx/zap (blank import) or call RegisterProvider")
	}
	return p.New(cfg)
}

// SetGlobal 设置全局 Logger 句柄
func SetGlobal(l Logger) {
	global = l
}

// GetGlobal 获取全局 Logger 句柄
// 未显式设置且无 provider 注册时返回 noopLogger，避免 panic
func GetGlobal() Logger {
	if global != nil {
		return global
	}
	globalOnce.Do(func() {
		if global != nil {
			return
		}
		l, err := New(DefaultConfig())
		if err != nil {
			return
		}
		global = l
	})
	if global == nil {
		return noopLogger{}
	}
	return global
}

// noopLogger 空实现 Logger，在无 provider 注册时作为安全回退，避免 panic
type noopLogger struct{}

func (n noopLogger) Debug(msg string, fields ...Field)                                             {}
func (n noopLogger) Info(msg string, fields ...Field)                                              {}
func (n noopLogger) Warn(msg string, fields ...Field)                                              {}
func (n noopLogger) Error(msg string, fields ...Field)                                             {}
func (n noopLogger) DPanic(msg string, fields ...Field)                                            {}
func (n noopLogger) Panic(msg string, fields ...Field)                                             {}
func (n noopLogger) Fatal(msg string, fields ...Field)                                             {}
func (n noopLogger) DebugContext(ctx context.Context, msg string, fields ...Field)                 {}
func (n noopLogger) InfoContext(ctx context.Context, msg string, fields ...Field)                  {}
func (n noopLogger) WarnContext(ctx context.Context, msg string, fields ...Field)                  {}
func (n noopLogger) ErrorContext(ctx context.Context, msg string, fields ...Field)                 {}
func (n noopLogger) DPanicContext(ctx context.Context, msg string, fields ...Field)                {}
func (n noopLogger) PanicContext(ctx context.Context, msg string, fields ...Field)                 {}
func (n noopLogger) FatalContext(ctx context.Context, msg string, fields ...Field)                 {}
func (n noopLogger) With(fields ...Field) Logger                                                   { return n }
func (n noopLogger) Named(name string) Logger                                                      { return n }
func (n noopLogger) Level() zapcore.Level                                                          { return zapcore.ErrorLevel }
func (n noopLogger) Sync() error                                                                   { return nil }
func (n noopLogger) Close() error                                                                  { return nil }
func (n noopLogger) Zap() *zap.Logger                                                              { return nil }
func (n noopLogger) Chain() *Builder                                                               { return &Builder{ctx: context.Background()} }
func (n noopLogger) DefaultService() string                                                        { return "" }
func (n noopLogger) DefaultOperation() string                                                      { return "" }
func (n noopLogger) DefaultTags() []string                                                         { return nil }
func (n noopLogger) ServiceName() string                                                           { return "" }
func (n noopLogger) InjectSpanAttrs(ctx context.Context, service, operation string, tags []string) {}

// ======== 全局便捷函数（使用全局 Logger）========

// Debug 使用全局 Logger 输出 debug 日志
func Debug(msg string, fields ...Field) { GetGlobal().Debug(msg, fields...) }

// Info 使用全局 Logger 输出 info 日志
func Info(msg string, fields ...Field) { GetGlobal().Info(msg, fields...) }

// Warn 使用全局 Logger 输出 warn 日志
func Warn(msg string, fields ...Field) { GetGlobal().Warn(msg, fields...) }

// Error 使用全局 Logger 输出 error 日志
func Error(msg string, fields ...Field) { GetGlobal().Error(msg, fields...) }

// DebugContext 使用全局 Logger 输出带 context 的 debug 日志
func DebugContext(ctx context.Context, msg string, fields ...Field) {
	GetGlobal().DebugContext(ctx, msg, fields...)
}

// InfoContext 使用全局 Logger 输出带 context 的 info 日志
func InfoContext(ctx context.Context, msg string, fields ...Field) {
	GetGlobal().InfoContext(ctx, msg, fields...)
}

// WarnContext 使用全局 Logger 输出带 context 的 warn 日志
func WarnContext(ctx context.Context, msg string, fields ...Field) {
	GetGlobal().WarnContext(ctx, msg, fields...)
}

// ErrorContext 使用全局 Logger 输出带 context 的 error 日志
func ErrorContext(ctx context.Context, msg string, fields ...Field) {
	GetGlobal().ErrorContext(ctx, msg, fields...)
}

// DPanicContext 使用全局 Logger 输出带 context 的 dpanic 日志
func DPanicContext(ctx context.Context, msg string, fields ...Field) {
	GetGlobal().DPanicContext(ctx, msg, fields...)
}

// PanicContext 使用全局 Logger 输出带 context 的 panic 日志
func PanicContext(ctx context.Context, msg string, fields ...Field) {
	GetGlobal().PanicContext(ctx, msg, fields...)
}

// FatalContext 使用全局 Logger 输出带 context 的 fatal 日志
func FatalContext(ctx context.Context, msg string, fields ...Field) {
	GetGlobal().FatalContext(ctx, msg, fields...)
}

// CloseGlobal 关闭全局 Logger
func CloseGlobal() {
	if global != nil {
		_ = global.Close()
		global = nil
	}
}
