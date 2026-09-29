// logx/zap/exporter_mq.go
// MQ 异步推送 exporter：将 span 序列化后推送到 MQ（Kafka / Pulsar），
// 由异步消费者读取并写入 Jaeger（或其它 OTLP 后端）。
//
// 架构：
//   - NewMQExporter：实现 sdktrace.SpanExporter，将 span JSON 序列化后写入 MQ topic
//   - NewMQTracerProvider：使用 MQ exporter 装配全局 TracerProvider（span → MQ）
//   - NewMQToJaegerConsumer：异步消费者，从 MQ 读取 span 并导出到 Jaeger（MQ → Jaeger）
//
// MQ 生产者与消费者通过 MessageQueue 接口抽象，内置 Kafka 实现（github.com/segmentio/kafka-go）。
package zap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gospacex/logx"
	"github.com/segmentio/kafka-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	cpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// ======================= MessageQueue 抽象 =======================

// MessageQueue MQ 生产者/消费者接口
// 默认实现：Kafka（NewKafkaProducer / NewKafkaConsumer）
// 可扩展：Pulsar 等（实现本接口即可接入）
type MessageQueue interface {
	// Produce 发送一条消息（key 可选，value 为载荷）
	Produce(ctx context.Context, key, value []byte) error
	// Consume 消费消息（由调用方循环调用直到返回 io.EOF 或 ctx 取消）
	Consume(ctx context.Context) (key, value []byte, err error)
	// Close 关闭连接
	Close() error
}

// ======================= Kafka 实现 =======================

// kafkaProducer kafka-go writer 封装
type kafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer 创建 Kafka 生产者
func NewKafkaProducer(cfg logx.MQConfig) (MessageQueue, error) {
	cfg.FillDefaults()
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("mq kafka: no brokers configured")
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false, // 同步写：保证 span 可靠投递（Close 时 flush）
		BatchSize:    1,     // 每条立即发送，减少 span 丢失
		BatchTimeout: 10 * time.Millisecond,
	}
	return &kafkaProducer{writer: w}, nil
}

func (p *kafkaProducer) Produce(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
}

func (p *kafkaProducer) Consume(ctx context.Context) ([]byte, []byte, error) {
	return nil, nil, fmt.Errorf("kafka producer cannot consume")
}

func (p *kafkaProducer) Close() error { return p.writer.Close() }

// kafkaConsumer kafka-go reader 封装
type kafkaConsumer struct {
	reader *kafka.Reader
}

// NewKafkaConsumer 创建 Kafka 消费者
func NewKafkaConsumer(cfg logx.MQConfig) (MessageQueue, error) {
	cfg.FillDefaults()
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("mq kafka: no brokers configured")
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  cfg.GroupID,
		Topic:    cfg.Topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	return &kafkaConsumer{reader: r}, nil
}

func (c *kafkaConsumer) Produce(ctx context.Context, key, value []byte) error {
	return fmt.Errorf("kafka consumer cannot produce")
}

func (c *kafkaConsumer) Consume(ctx context.Context) ([]byte, []byte, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, nil, err
	}
	return msg.Key, msg.Value, nil
}

func (c *kafkaConsumer) Close() error { return c.reader.Close() }

// ======================= MQ Span 传输结构 =======================

// MQSpan 传输用 span 结构（JSON）
type MQSpan struct {
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id,omitempty"`
	Name          string            `json:"name"`
	StartUnixNano uint64            `json:"start_unix_nano"`
	EndUnixNano   uint64            `json:"end_unix_nano"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	// Resource 资源属性（TracerProvider resource，含 service.name 等；
	// 消费端回写 Jaeger/OTLP 时还原，否则 Jaeger 归为 OTLPResourceNoServiceName）
	Resource      map[string]string `json:"resource,omitempty"`
	Events        []MQSpanEvent     `json:"events,omitempty"`
	StatusMessage string            `json:"status_message,omitempty"`
}

// MQSpanEvent span 事件（含日志通过 OTelCore 注入的事件）
type MQSpanEvent struct {
	Name         string            `json:"name"`
	TimeUnixNano uint64            `json:"time_unix_nano"`
	Attributes   map[string]string `json:"attributes,omitempty"`
}

// marshalSpan 将 ReadOnlySpan 序列化为 JSON
func marshalSpan(span sdktrace.ReadOnlySpan) ([]byte, error) {
	sc := span.SpanContext()
	attrs := make(map[string]string, len(span.Attributes()))
	for _, kv := range span.Attributes() {
		attrs[string(kv.Key)] = kv.Value.AsString()
	}

	// 资源属性（TracerProvider 的 resource，含 service.name）：随 span 传输，
	// 消费端回写时还原，Jaeger 才能正确归组服务名
	resourceAttrs := map[string]string{}
	if res := span.Resource(); res != nil {
		for _, kv := range res.Attributes() {
			resourceAttrs[string(kv.Key)] = kv.Value.AsString()
		}
	}

	events := make([]MQSpanEvent, 0, len(span.Events()))
	for _, ev := range span.Events() {
		evAttrs := make(map[string]string, len(ev.Attributes))
		for _, kv := range ev.Attributes {
			evAttrs[string(kv.Key)] = kv.Value.AsString()
		}
		events = append(events, MQSpanEvent{
			Name:         ev.Name,
			TimeUnixNano: uint64(ev.Time.UnixNano()),
			Attributes:   evAttrs,
		})
	}

	ms := MQSpan{
		TraceID:       sc.TraceID().String(),
		SpanID:        sc.SpanID().String(),
		ParentSpanID:  span.Parent().SpanID().String(),
		Name:          span.Name(),
		StartUnixNano: uint64(span.StartTime().UnixNano()),
		EndUnixNano:   uint64(span.EndTime().UnixNano()),
		Attributes:    attrs,
		Resource:      resourceAttrs,
		Events:        events,
		StatusMessage: span.Status().Description,
	}
	return json.Marshal(ms)
}

// ======================= MQ SpanExporter =======================

// mqSpanExporter 将 span 序列化后推送至 MQ
type mqSpanExporter struct {
	queue MessageQueue
	opts  mqExporterOptions
}

type mqExporterOptions struct {
	KeyPrefix string
}

// MQExporterOption MQ exporter 配置项
type MQExporterOption func(*mqExporterOptions)

// WithMQKeyPrefix 设置 topic key 前缀
func WithMQKeyPrefix(prefix string) MQExporterOption {
	return func(o *mqExporterOptions) { o.KeyPrefix = prefix }
}

// NewMQExporter 创建 MQ span exporter
// queue 参数：生产者 MessageQueue（如 NewKafkaProducer）
func NewMQExporter(queue MessageQueue, opts ...MQExporterOption) (sdktrace.SpanExporter, error) {
	if queue == nil {
		return nil, fmt.Errorf("mq exporter: nil MessageQueue")
	}
	o := mqExporterOptions{KeyPrefix: "otel"}
	for _, opt := range opts {
		opt(&o)
	}
	return &mqSpanExporter{queue: queue, opts: o}, nil
}

// ExportSpans 将 spans 批量写入 MQ
func (e *mqSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, span := range spans {
		data, err := marshalSpan(span)
		if err != nil {
			return fmt.Errorf("mq marshal span: %w", err)
		}
		key := []byte(fmt.Sprintf("%s-%s", e.opts.KeyPrefix, span.SpanContext().TraceID().String()))
		if err := e.queue.Produce(ctx, key, data); err != nil {
			return fmt.Errorf("mq produce: %w", err)
		}
	}
	return nil
}

// Shutdown 关闭 MQ 连接
func (e *mqSpanExporter) Shutdown(ctx context.Context) error {
	return e.queue.Close()
}

// ForceFlush 无缓冲，直接返回
func (e *mqSpanExporter) ForceFlush(ctx context.Context) error { return nil }

// ======================= MQ TracerProvider =======================

// NewMQTracerProvider 使用 MQ exporter 装配全局 TracerProvider（span → Kafka 直推）
// 统一装配入口；内部转调 SetupTracerProviderWithMQ。
// 无论 cfg.Exporter 取值如何，均强制路由到 mq（保持既有"MQ 专用入口"语义，不受 FillDefaults 默认值影响）
func NewMQTracerProvider(cfg logx.OTelConfig, mqCfg logx.MQConfig) (*TracerSetup, error) {
	cfg.Exporter = "mq"
	return SetupTracerProviderWithMQ(cfg, mqCfg)
}

// buildProducer 根据 MQ 类型构建生产者
func buildProducer(cfg logx.MQConfig) (MessageQueue, error) {
	switch cfg.Type {
	case "kafka", "":
		return NewKafkaProducer(cfg)
	case "pulsar":
		return nil, fmt.Errorf("pulsar producer not yet wired; configure MQ type=kafka")
	default:
		return nil, fmt.Errorf("unsupported mq type: %s", cfg.Type)
	}
}

// ======================= MQ → Jaeger 异步消费者 =======================

// MQToJaegerConsumer MQ → Jaeger 异步消费者
// 从 MQ 读取 span JSON，转换为 OTLP proto 后通过 OTLP HTTP 协议写入 Jaeger
type MQToJaegerConsumer struct {
	queue    MessageQueue
	endpoint string
	client   *http.Client

	mu    sync.Mutex
	batch []*MQSpan
}

// NewMQToJaegerConsumer 创建异步消费者：
// 从 MQ 读取 span JSON，转换为 OTLP proto 后通过 OTLP HTTP 写入 Jaeger。
// endpoint：Jaeger OTLP HTTP 端点（如 http://localhost:4318）
func NewMQToJaegerConsumer(queue MessageQueue, endpoint string) (*MQToJaegerConsumer, error) {
	if queue == nil {
		return nil, fmt.Errorf("mq consumer: nil MessageQueue")
	}
	return &MQToJaegerConsumer{
		queue:    queue,
		endpoint: strings.TrimRight(endpoint, "/"),
		client:   &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// RunMQLoop 运行消费循环，直到 ctx 取消
// 每消费一条 span 立即转发（小批量场景足够），数据写入 Jaeger。
// 坏消息（JSON 解析失败 / ID 长度非法）跳过并计数，不中断循环——
// topic 上的杂消息不应让整条回写链路停摆；转发失败（网络/4xx）返回 error。
func (c *MQToJaegerConsumer) RunMQLoop(ctx context.Context) error {
	skipped := 0
	for {
		_, value, err := c.queue.Consume(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // 正常退出
			}
			return fmt.Errorf("mq consume: %w", err)
		}
		var ms MQSpan
		if err := json.Unmarshal(value, &ms); err != nil {
			skipped++
			continue // 跳过坏消息
		}
		if !validSpanIDs(ms) {
			// Jaeger OTLP 对非法 ID（长度非 32/16 hex）返回 400 "unmarshal: invalid TraceID length"，
			// 提前在消费端过滤，避免单条坏消息 400 中断整个回写循环
			skipped++
			continue
		}
		if err := c.exportOne(ctx, ms); err != nil {
			return fmt.Errorf("%w (skipped %d malformed messages)", err, skipped)
		}
	}
}

// validSpanIDs 校验 span 的 trace/span ID 是否满足 OTLP 定长要求（16/8 字节 hex）
func validSpanIDs(ms MQSpan) bool {
	return len(ms.TraceID) == 32 && len(ms.SpanID) == 16
}

// exportOne 将单条 span 通过 OTLP HTTP 写入 Jaeger
func (c *MQToJaegerConsumer) exportOne(ctx context.Context, ms MQSpan) error {
	rs := buildResourceSpans(ms)
	req := &cpb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{rs},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("mq otlp marshal: %w", err)
	}

	url := c.endpoint + "/v1/traces"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("mq otlp post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		// OTLP partial_success 时响应体含逐条拒绝原因（protobuf 二进制，
		// 截断打印辅助定位；正常 200/202 不读 body）
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("mq otlp post: unexpected status %d: %s", resp.StatusCode, truncate(body, 200))
	}
	return nil
}

// truncate 截断字节到 maxLen（辅助错误信息可读性）
func truncate(b []byte, maxLen int) string {
	if len(b) <= maxLen {
		return string(b)
	}
	return string(b[:maxLen]) + "...(truncated)"
}

// Close 关闭消费者
func (c *MQToJaegerConsumer) Close(ctx context.Context) error {
	return nil
}

// buildResourceSpans 将 MQSpan 转换为 OTLP ResourceSpans
// 资源属性取自 MQSpan.Resource（生产端 TracerProvider resource）；
// 旧格式消息（无 resource 字段）回退 service.name=logx-mq-legacy，
// 避免 Jaeger 归入难辨认的 OTLPResourceNoServiceName 分组
func buildResourceSpans(ms MQSpan) *tracepb.ResourceSpans {
	resAttrs := ms.Resource
	if len(resAttrs) == 0 {
		resAttrs = map[string]string{"service.name": "logx-mq-legacy"}
	}
	resKV := make([]*commonpb.KeyValue, 0, len(resAttrs))
	for k, v := range resAttrs {
		resKV = append(resKV, &commonpb.KeyValue{
			Key: k,
			Value: &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{StringValue: v},
			},
		})
	}

	attrs := make([]*commonpb.KeyValue, 0, len(ms.Attributes))
	for k, v := range ms.Attributes {
		attrs = append(attrs, &commonpb.KeyValue{
			Key: k,
			Value: &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{StringValue: v},
			},
		})
	}

	events := make([]*tracepb.Span_Event, 0, len(ms.Events))
	for _, ev := range ms.Events {
		evAttrs := make([]*commonpb.KeyValue, 0, len(ev.Attributes))
		for k, v := range ev.Attributes {
			evAttrs = append(evAttrs, &commonpb.KeyValue{
				Key: k,
				Value: &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{StringValue: v},
				},
			})
		}
		events = append(events, &tracepb.Span_Event{
			TimeUnixNano: ev.TimeUnixNano,
			Name:         ev.Name,
			Attributes:   evAttrs,
		})
	}

	span := &tracepb.Span{
		TraceId:           idToBytes(ms.TraceID),
		SpanId:            idToBytes(ms.SpanID),
		ParentSpanId:      idToBytes(ms.ParentSpanID),
		Name:              ms.Name,
		StartTimeUnixNano: ms.StartUnixNano,
		EndTimeUnixNano:   ms.EndUnixNano,
		Attributes:        attrs,
		Events:            events,
	}

	return &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{Attributes: resKV},
		ScopeSpans: []*tracepb.ScopeSpans{
			{
				Spans: []*tracepb.Span{span},
			},
		},
	}
}

// idToBytes 将十六进制 ID 字符串转换为字节数组（16 字节 trace / 8 字节 span）
func idToBytes(hex string) []byte {
	if hex == "" || hex == "0000000000000000" {
		return nil
	}
	var out []byte
	for i := 0; i+1 < len(hex); i += 2 {
		var b byte
		fmt.Sscanf(hex[i:i+2], "%02x", &b)
		out = append(out, b)
	}
	return out
}
