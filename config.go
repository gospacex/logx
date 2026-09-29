// logx/config.go
// logx 配置结构：日志配置 + OTel 配置 + MQ 推送配置
package logx

import (
	"time"

	"github.com/gospacex/traceflag"
)

// Config 日志配置
// 对应 config.yaml 中的 logx: 段
type Config struct {
	// Level 日志级别：debug / info / warn / error / dpanic / panic / fatal
	Level string `mapstructure:"level" yaml:"level"`
	// Format 输出格式：console / json
	Format string `mapstructure:"format" yaml:"format"`
	// Output 输出目标：stdout / file / both
	Output string `mapstructure:"output" yaml:"output"`
	// FilePath 文件输出路径（Output=file/both 时生效）
	FilePath string `mapstructure:"file_path" yaml:"file_path"`
	// MaxSize 单个日志文件最大大小（MB）
	MaxSize int `mapstructure:"max_size" yaml:"max_size"`
	// MaxBackups 保留的旧日志文件最大数量
	MaxBackups int `mapstructure:"max_backups" yaml:"max_backups"`
	// MaxAge 保留的旧日志文件最大天数
	MaxAge int `mapstructure:"max_age" yaml:"max_age"`
	// Compress 旧日志文件是否压缩
	Compress bool `mapstructure:"compress" yaml:"compress"`
	// Caller 是否在日志中记录调用位置
	Caller bool `mapstructure:"caller" yaml:"caller"`
	// Development 开发模式（启用更友好的编码与堆栈跟踪）
	Development bool `mapstructure:"development" yaml:"development"`
	// ServiceName 服务名（用于 OTel 资源属性）
	ServiceName string `mapstructure:"service_name" yaml:"service_name"`
	// ServiceVersion 服务版本（用于 OTel 资源属性）
	ServiceVersion string `mapstructure:"service_version" yaml:"service_version"`
	// Environment 环境（development / production / test，用于 OTel 资源属性）
	Environment string `mapstructure:"environment" yaml:"environment"`
	// DefaultService 缺省 Service（日志字段 log.service；Builder 回退链）
	DefaultService string `mapstructure:"default_service" yaml:"default_service"`
	// DefaultOperation 缺省 Operation（日志字段 log.operation）
	DefaultOperation string `mapstructure:"default_operation" yaml:"default_operation"`
	// DefaultTags 缺省 Tags（与链式 Tags 并集去重后写 log.tags）
	DefaultTags []string `mapstructure:"default_tags" yaml:"default_tags"`
	// OTel 调用链集成配置
	OTel OTelConfig `mapstructure:"otel" yaml:"otel"`
	// MQ 推送配置（OTel exporter=mq 时生效）
	MQ MQConfig `mapstructure:"mq" yaml:"mq"`
}

// DefaultConfig 返回一份带默认值的配置
func DefaultConfig() Config {
	return Config{
		Level:          "info",
		Format:         "console",
		Output:         "stdout",
		FilePath:       "./logs/app.log",
		MaxSize:        100,
		MaxBackups:     3,
		MaxAge:         7,
		Compress:       true,
		Caller:         true,
		Development:    false,
		ServiceName:    "logx",
		ServiceVersion: "1.0.0",
		Environment:    "development",
		OTel: OTelConfig{
			Enabled:     false,
			Exporter:    "jaeger",
			ServiceName: "",
			Sampler: SamplerConfig{
				Type:  "parentbased_traceidratio",
				Ratio: 1.0,
			},
		},
	}
}

// FillDefaults 用默认值填充为零值的字段
func (c *Config) FillDefaults() {
	def := DefaultConfig()
	if c.Level == "" {
		c.Level = def.Level
	}
	if c.Format == "" {
		c.Format = def.Format
	}
	if c.Output == "" {
		c.Output = def.Output
	}
	if c.FilePath == "" {
		c.FilePath = def.FilePath
	}
	if c.MaxSize <= 0 {
		c.MaxSize = def.MaxSize
	}
	if c.MaxBackups <= 0 {
		c.MaxBackups = def.MaxBackups
	}
	if c.MaxAge <= 0 {
		c.MaxAge = def.MaxAge
	}
	if c.ServiceName == "" {
		c.ServiceName = def.ServiceName
	}
	if c.ServiceVersion == "" {
		c.ServiceVersion = def.ServiceVersion
	}
	if c.Environment == "" {
		c.Environment = def.Environment
	}

	// OTel 子配置继承顶层服务信息（若未单独指定）
	if c.OTel.ServiceName == "" && c.ServiceName != "" {
		c.OTel.ServiceName = c.ServiceName
	}
	if c.OTel.ServiceVersion == "" && c.ServiceVersion != "" {
		c.OTel.ServiceVersion = c.ServiceVersion
	}
	if c.OTel.Environment == "" && c.Environment != "" {
		c.OTel.Environment = c.Environment
	}

	c.OTel.FillDefaults()

	// 接入 hubx 统一调用链开关：组件自身配置（otel.enable_trace 主键，
	// otel.enabled 回退别名）为主开关，全局总闸 traceflag.SetEnabled(false)
	// 可一键熔断 logx 的日志→Span 注入（见 zap.New 里的 NewOTelCore
	// 包装）。归一化后 Enabled 是驱动侧的唯一真源。FillDefaults 是所有
	// 驱动构造的必经之路，注册放这一处即可覆盖全部日志后端。
	c.OTel.Enabled = c.OTel.EnableTrace || c.OTel.Enabled
	c.OTel.Enabled = traceflag.Apply("logx", c.OTel.Enabled)
}

// ======================= OTel 配置 =======================

// OTelConfig OTel 调用链集成配置
type OTelConfig struct {
	// EnableTrace 统一调用链开关键（主键）。
	// 未配置时回退到 Enabled（旧键 enabled，保留为兼容别名）；
	// 二者任一为 true 即启用 OTel 集成。
	EnableTrace bool `mapstructure:"enable_trace" yaml:"enable_trace"`
	// Enabled 是否开启 OTel 调用链集成（旧键，兼容别名）
	Enabled bool `mapstructure:"enabled" yaml:"enabled"`
	// Exporter 数据推送目标：jaeger / mq / otlp / stdout
	Exporter string `mapstructure:"exporter" yaml:"exporter"`
	// ServiceName 覆盖全局 ServiceName（用于 OTel 资源属性）
	ServiceName string `mapstructure:"service_name" yaml:"service_name"`
	// ServiceVersion 覆盖全局 ServiceVersion
	ServiceVersion string `mapstructure:"service_version" yaml:"service_version"`
	// Environment 覆盖全局 Environment
	Environment string `mapstructure:"environment" yaml:"environment"`
	// Endpoint 统一端点（jaeger collector / otlp endpoint / mq bootstrap 地址）
	Endpoint string `mapstructure:"endpoint" yaml:"endpoint"`
	// Jaeger Jaeger 推送配置（Exporter=jaeger 时生效）
	Jaeger JaegerConfig `mapstructure:"jaeger" yaml:"jaeger"`
	// OTLP OTLP 推送配置（Exporter=otlp 时生效）
	OTLP OTLPConfig `mapstructure:"otlp" yaml:"otlp"`
	// Sampler 采样配置
	Sampler SamplerConfig `mapstructure:"sampler" yaml:"sampler"`
}

// FillDefaults 填充 OTel 配置默认值
func (o *OTelConfig) FillDefaults() {
	if o.Exporter == "" {
		o.Exporter = "jaeger"
	}
	if o.ServiceName == "" {
		o.ServiceName = "logx"
	}
	if o.ServiceVersion == "" {
		o.ServiceVersion = "1.0.0"
	}
	if o.Environment == "" {
		o.Environment = "development"
	}
	if o.Sampler.Type == "" {
		o.Sampler.Type = "parentbased_traceidratio"
	}
	if o.Sampler.Ratio == 0 {
		o.Sampler.Ratio = 1.0
	}
}

// JaegerConfig Jaeger 推送配置
type JaegerConfig struct {
	// Endpoint Jaeger collector HTTP 端点，如 http://localhost:14268/api/traces
	Endpoint string `mapstructure:"endpoint" yaml:"endpoint"`
	// AgentHost Jaeger agent UDP 主机（使用 agent 上报时）
	AgentHost string `mapstructure:"agent_host" yaml:"agent_host"`
	// AgentPort Jaeger agent UDP 端口（默认 6831）
	AgentPort int `mapstructure:"agent_port" yaml:"agent_port"`
}

// OTLPConfig OTLP 推送配置
type OTLPConfig struct {
	// Endpoint OTLP endpoint，如 localhost:4317 / http://localhost:4318
	Endpoint string `mapstructure:"endpoint" yaml:"endpoint"`
	// Insecure 是否使用非 TLS 连接
	Insecure bool `mapstructure:"insecure" yaml:"insecure"`
	// Headers 附加请求头
	Headers map[string]string `mapstructure:"headers" yaml:"headers"`
}

// SamplerConfig 采样配置
type SamplerConfig struct {
	// Type 采样器类型：always / never / traceidratio / parentbased_traceidratio
	Type string `mapstructure:"type" yaml:"type"`
	// Ratio 采样比例（0-1），traceidratio / parentbased_traceidratio 时生效
	Ratio float64 `mapstructure:"ratio" yaml:"ratio"`
}

// ======================= MQ 配置 =======================

// MQConfig MQ 推送配置（OTel exporter=mq 时生效）
type MQConfig struct {
	// Type MQ 类型：kafka / pulsar
	Type string `mapstructure:"type" yaml:"type"`
	// Brokers Broker 地址列表（kafka: host:port；pulsar: pulsar://host:port）
	Brokers []string `mapstructure:"brokers" yaml:"brokers"`
	// Topic 推送 Topic
	Topic string `mapstructure:"topic" yaml:"topic"`
	// GroupID 消费者组（异步消费写入 Jaeger 时使用）
	GroupID string `mapstructure:"group_id" yaml:"group_id"`
	// MaxRetries 重试次数
	MaxRetries int `mapstructure:"max_retries" yaml:"max_retries"`
	// Timeout 写入超时
	Timeout time.Duration `mapstructure:"timeout" yaml:"timeout"`
}

// FillDefaults 填充 MQ 配置默认值
func (m *MQConfig) FillDefaults() {
	if m.Type == "" {
		m.Type = "kafka"
	}
	if m.Topic == "" {
		m.Topic = "otel-traces"
	}
	if m.GroupID == "" {
		m.GroupID = "logx-otel-consumer"
	}
	if m.MaxRetries <= 0 {
		m.MaxRetries = 3
	}
	if m.Timeout <= 0 {
		m.Timeout = 5 * time.Second
	}
}
