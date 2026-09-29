// logx/zap/check_edge_test.go
// OTelCore.Check 启用/禁用分支、stringify Stringer 分支（经 ReflectType 驱动）、
// buildMQExporter kafka 成功通路
package zap

import (
	"testing"

	"github.com/gospacex/logx"
	"go.uber.org/zap/zapcore"
)

// TestOTelCoreCheckBothBranches 下游禁用时 Check 不加 Core（Write 无副作用）；
// 启用时 AddCore 包装（Write 到达下游）
// CheckedEntry 无公开构造器，但 CheckedEntry.AddCore 对 nil 接收者安全（内部分配），
// 故直接传 nil 驱动被测方法，用返回值断言
func TestOTelCoreCheckBothBranches(t *testing.T) {
	// 禁用：Check 不加 Core → 原样返回入参（nil）；下游不被触达
	disabled := &recordingCore{} // enabled=false
	c := NewOTelCore(disabled)
	if out := c.Check(zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"}, nil); out != nil {
		t.Errorf("下游禁用时 Check = %v, want 原样返回 nil（未挂 Core）", out)
	}

	// 启用：Check 经 AddCore 挂 Core → ce.Write 经 OTelCore 转发到达下游
	enabled := newRecordingCore()
	c2 := NewOTelCore(enabled)
	ce2 := c2.Check(zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"}, nil)
	if ce2 == nil {
		t.Fatal("启用时 Check 返回 nil CheckedEntry")
	}
	ce2.Write()
	if len(enabled.entries) != 1 {
		t.Errorf("下游启用时未加 Core：entries=%d, want 1", len(enabled.entries))
	}
}

// TestStringifyViaReflectStringer ReflectType + 自定义 Stringer（非 string）→
// stringify 走 s.String() 分支（而非 "nil" 兜底）
func TestStringifyViaReflectStringer(t *testing.T) {
	f := zapcore.Field{Key: "s", Type: zapcore.ReflectType, Interface: strVal("sv")}
	if s := stringify(strVal("sv")); s != "sv" {
		t.Errorf("stringify(Stringer) = %q, want sv", s)
	}
	if s := stringify(12345); s != "nil" {
		t.Errorf("stringify(非 Stringer) = %q, want 字面量 nil", s)
	}
	if _, ok := f.Interface.(string); ok {
		t.Error("前置 sanity：strVal 不应是 string")
	}
}

// TestBuildMQExporterKafkaSuccess kafka 带 brokers 的完整成功通路（buildProducer → NewMQExporter）
func TestBuildMQExporterKafkaSuccess(t *testing.T) {
	exp, err := buildMQExporter(logx.MQConfig{
		Type:    "kafka",
		Brokers: []string{"localhost:1"},
		Topic:   "t",
	})
	if err != nil {
		t.Fatalf("buildMQExporter(kafka) = %v, want 成功构造", err)
	}
	if exp == nil {
		t.Error("buildMQExporter 返回 nil exporter")
	}
	// Shutdown 经 queue.Close 收口（kafka writer 幂等关闭，不触网）
	if err := exp.Shutdown(nil); err != nil {
		t.Errorf("Shutdown = %v, want nil", err)
	}
}

// TestBuildMQExporterProducerError 类型路由错误传播：pulsar 未实现 →
// buildProducer 报错，buildMQExporter 原样上抛（不吞错）
func TestBuildMQExporterProducerError(t *testing.T) {
	_, err := buildMQExporter(logx.MQConfig{
		Type:    "pulsar",
		Brokers: []string{"localhost:1"},
		Topic:   "t",
	})
	if err == nil {
		t.Fatal("buildMQExporter(pulsar) = nil error, want 未实现错误")
	}
}
