// logx/config_read_test.go
// ReadConfig / GetConfig 全局配置句柄：加载成功句柄可用、HUBX_CONFIG 环境变量通路、
// GetConfig 读锁保护下的并发访问
package logx

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gospacex/configx"
)

// TestReadConfigAndGetConfig 加载本地配置（远程默认关闭、文件缺失走 emptyConfig 兜底，离线安全）
// → GetConfig 返回同一句柄；再传空内容文件仍成功（缺文件非致命）
func TestReadConfigAndGetConfig(t *testing.T) {
	empty := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(empty, []byte("# empty\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ReadConfig(configx.ConfigOptions{ConfigFile: empty}); err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	if c := GetConfig(); c == nil {
		t.Error("GetConfig() = nil, want 已加载句柄")
	}
}

// TestReadConfigHUBXConfigEnv HUBX_CONFIG 环境变量指定配置文件通路
func TestReadConfigHUBXConfigEnv(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "env-config.yaml")
	if err := os.WriteFile(p, []byte("logx:\n  level: info\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HUBX_CONFIG", p)
	if err := ReadConfig(); err != nil {
		t.Fatalf("ReadConfig(HUBX_CONFIG): %v", err)
	}
	if c := GetConfig(); c == nil {
		t.Error("GetConfig() = nil, want HUBX_CONFIG 加载的句柄")
	}
}

// TestGetConfigConcurrent GetConfig 读锁保护：并发读不触发 race（配合 -race 验证）
func TestGetConfigConcurrent(t *testing.T) {
	if err := ReadConfig(configx.ConfigOptions{ConfigFile: filepath.Join(t.TempDir(), "x.yaml")}); err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = GetConfig()
		}()
	}
	wg.Wait()
}
