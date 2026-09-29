// logx/example/02-file-rotate/main.go
// 用例 02：文件输出 + lumberjack 滚动轮转（零依赖，无需 docker）
//
// 前置条件：无
// 运行：go run .
//
// 覆盖功能矩阵：
//
//	§2 输出：Output=both（stdout + 文件 Tee）、json 格式、lumberjack 滚动
//	   MaxSize=1（MB，用例专用小值以便快速触发轮转）、MaxAge/MaxBackups/Compress
//	   Error 级自动堆栈
//
// 验证：运行后 `ls logs/` 应出现 app-<时间戳>.log.gz（lumberjack 压缩轮转证据，
// 注意 lumberjack 轮转文件命名带时间戳，非 app.log.1.gz）
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap" // 注册默认 provider
)

func main() {
	cfg := logx.DefaultConfig()
	cfg.Level = "debug"
	cfg.Format = "json" // 文件落盘用 JSON（机器可解析）
	cfg.Output = "both" // stdout + 文件 Tee
	cfg.FilePath = "logs/app.log"
	cfg.MaxSize = 1     // MB：用例专用小值，快速触发轮转
	cfg.MaxBackups = 3  // 保留 3 份
	cfg.MaxAge = 7      // 保留 7 天
	cfg.Compress = true // 旧文件 gzip 压缩

	logger, err := logx.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logx.New failed: %v\n", err)
		return
	}
	defer logger.Close()
	logx.SetGlobal(logger)

	// 写 4MB 日志（每行 ~1KB 有效负载），触发 4 次以上轮转
	const payload = "abcdefghijklmnopqrstuvwxyz0123456789" // 32B 基块
	line := buildLine(payload)
	for i := 0; i < 4096; i++ {
		if i%256 == 255 {
			logger.Error("error: simulated failure",
				logx.Int("seq", i),
				logx.String("detail", "stock service returned 503"),
			) // Error 级自动附带堆栈（zap AddStacktrace）
		}
		logger.Info(line, logx.Int("seq", i))
	}
	logger.Sync()

	fmt.Printf("done: %d lines written to %s\n", 4096, cfg.FilePath)
	fmt.Println("verify rotation: ls", filepath.Dir(cfg.FilePath), "(expect app-<时间戳>.log.gz 压缩轮转文件)")
}

// buildLine 构造 ~1KB 的有效负载行（触发轮转需总量超过 MaxSize）
func buildLine(block string) string {
	var b []byte
	for len(b) < 1024 {
		b = append(b, block...)
	}
	return string(b)
}
