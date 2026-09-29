// logx/configx_entry_test.go
// 统一配置入口三件套：ReadAndCreate / MustReadAndCreate。
//
// 放在外部测试包（package logx_test）：需要 blank import logx/zap 才能真正
// 建出 logger（内部测试包不能 import logx/zap，见 builder_ext_test.go 注释）。
package logx_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gospacex/configx"
	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap"
)

const logxSectionYAML = `
logx:
  level: info
  format: json
  output: stdout
  service_name: logx-entry-test
  service_version: "2.3.4"
  environment: staging
  caller: false
  otel:
    enabled: false
`

func writeLogxConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

// TestReadAndCreate_ReturnsLogger 验证入口把「读配置 + 取段 + 建实例」一次做完。
func TestReadAndCreate_ReturnsLogger(t *testing.T) {
	l, err := logx.ReadAndCreate(configx.ConfigOptions{ConfigFile: writeLogxConfig(t, logxSectionYAML)})
	if err != nil {
		t.Fatalf("ReadAndCreate: %v", err)
	}
	if l == nil {
		t.Fatal("ReadAndCreate returned nil logger")
	}
	// 可用性：真实调用一次不 panic（zap 驱动写 stdout）。
	l.Info("logx entry smoke")
}

// ReadAndCreate 内部复用本包的 ReadConfig，因此全局配置句柄应同时就位 ——
// 这是它相对"直接调 configx.ReadConfig"的额外收益，调用方随后还能用
// GetConfig 做进一步取段。
func TestReadAndCreate_AlsoPopulatesGlobalConfig(t *testing.T) {
	if _, err := logx.ReadAndCreate(configx.ConfigOptions{ConfigFile: writeLogxConfig(t, logxSectionYAML)}); err != nil {
		t.Fatalf("ReadAndCreate: %v", err)
	}
	if c := logx.GetConfig(); c == nil {
		t.Fatal("GetConfig() = nil after ReadAndCreate")
	}
}

// TestLogxSectionDecodesIntoConfig 守住 logx 段的解码：字段解不到就只会
// 表现为 FillDefaults 补上的默认值（日志级别/输出目标悄悄变了）。
func TestLogxSectionDecodesIntoConfig(t *testing.T) {
	if err := logx.ReadConfig(configx.ConfigOptions{ConfigFile: writeLogxConfig(t, logxSectionYAML)}); err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	var cfg logx.Config
	if err := logx.GetConfig().Unmarshal("logx", &cfg); err != nil {
		t.Fatalf("Unmarshal logx section: %v", err)
	}
	if cfg.Level != "info" {
		t.Errorf("Level = %q, want info", cfg.Level)
	}
	if cfg.Format != "json" {
		t.Errorf("Format = %q, want json", cfg.Format)
	}
	if cfg.Output != "stdout" {
		t.Errorf("Output = %q, want stdout", cfg.Output)
	}
	if cfg.ServiceName != "logx-entry-test" {
		t.Errorf("ServiceName = %q, want logx-entry-test", cfg.ServiceName)
	}
	if cfg.ServiceVersion != "2.3.4" {
		t.Errorf("ServiceVersion = %q, want 2.3.4", cfg.ServiceVersion)
	}
	if cfg.Environment != "staging" {
		t.Errorf("Environment = %q, want staging", cfg.Environment)
	}
	if cfg.Caller {
		t.Error("Caller = true, want false")
	}
}

// 段缺失必须报错，而不是静默建一个全默认 logger。
func TestReadAndCreate_MissingSectionFails(t *testing.T) {
	l, err := logx.ReadAndCreate(configx.ConfigOptions{
		ConfigFile: writeLogxConfig(t, "app:\n  name: demo\n"),
	})
	if err == nil {
		t.Fatal("ReadAndCreate with no logx section must fail")
	}
	if l != nil {
		t.Error("logger must be nil when ReadAndCreate fails")
	}
	if !strings.Contains(err.Error(), "logx") {
		t.Errorf("error %q should name the logx section", err.Error())
	}
}

func TestMustReadAndCreate_ReturnsLogger(t *testing.T) {
	if l := logx.MustReadAndCreate(configx.ConfigOptions{ConfigFile: writeLogxConfig(t, logxSectionYAML)}); l == nil {
		t.Fatal("MustReadAndCreate returned nil logger")
	}
}
