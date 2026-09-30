// main.go — 程序入口、信号处理、优雅退出
//
// 程序生命周期：
//   1. 加载配置 config.yaml
//   2. 创建日志记录器（slog，等级由配置决定）
//   3. 创建 Hysteria v2 本地 API 客户端
//   4. 创建带签名的 Server HTTP 客户端
//   5. 创建本地缓存（加载快照，供 reporter 使用）
//   6. 创建 Reporter / Kicker / Heartbeat 并启动
//   7. 启动补报 goroutine（从缓存重放未上报流量）
//   8. 等待信号（SIGTERM/SIGHUP），优雅退出
//
// 信号处理：
//   - SIGTERM / SIGINT → 停止所有协程 → 等待缓存写入 → 退出
//   - SIGHUP → 重新加载配置（不重启进程，仅更新 intervals 和 server 配置）
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "/etc/hy-client/config.yaml", "配置文件路径")
	versionFlag := flag.Bool("version", false, "打印版本并退出")
	flag.Parse()

	if *versionFlag {
		fmt.Println("hy-client version 1.0.0")
		os.Exit(0)
	}

	cfg, err := Load(*configPath)
	if err != nil {
		slog.Error("配置加载失败", "error", err)
		os.Exit(1)
	}

	setupLogging(cfg.Log.LogLevel())

	slog.Info("hy-client 启动",
		"version", "1.0.0",
		"config", *configPath,
	)

	hyClient := NewHysteriaClient(cfg.Hysteria.APIURL, cfg.Hysteria.Secret)
	serverClient := newHTTPClient(cfg.ServerURL, cfg.ServerSecret, cfg.NodeID)

	cache, err := NewCache(cfg.Cache.Path, cfg.Cache.MaxEntries)
	if err != nil {
		slog.Error("缓存初始化失败", "error", err)
		os.Exit(1)
	}

	reporter := NewReporter(cfg, hyClient, serverClient, cache)
	if err := reporter.LoadSnapshot(); err != nil {
		slog.Warn("快照加载失败，使用空快照", "error", err)
	}

	kicker := NewKicker(cfg, hyClient, serverClient)
	heartbeat := NewHeartbeat(cfg, hyClient, serverClient, "1.0.0")

	reporter.Start()
	kicker.Start()
	heartbeat.Start()

	go runCacheReplay(cfg, serverClient, cache)
	setupSignals(cfg, reporter, kicker, heartbeat, cache)

	slog.Info("hy-client 进入运行状态")
	select {}
}

func setupLogging(level slog.Level) {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	slog.SetDefault(slog.New(handler))
}

func setupSignals(cfg *Config, reporter *Reporter, kicker *Kicker, heartbeat *Heartbeat, cache *Cache) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	go func() {
		for {
			sig := <-sigChan
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				slog.Info("收到终止信号，开始优雅退出")
				shutdown(reporter, kicker, heartbeat, cache)
				os.Exit(0)
			case syscall.SIGHUP:
				slog.Info("收到 SIGHUP，重新加载配置")
				newCfg, err := Load("/etc/hy-client/config.yaml")
				if err != nil {
					slog.Error("配置重载失败", "error", err)
					continue
				}
				cfg = newCfg
				slog.Info("配置重载成功",
					"traffic_interval", cfg.Intervals.TrafficReport.Duration,
					"kick_interval", cfg.Intervals.KickCheck.Duration,
					"heartbeat_interval", cfg.Intervals.Heartbeat.Duration,
				)
			}
		}
	}()
}

func shutdown(reporter *Reporter, kicker *Kicker, heartbeat *Heartbeat, cache *Cache) {
	reporter.Stop()
	kicker.Stop()
	heartbeat.Stop()
	slog.Info("所有组件已停止")
}

func runCacheReplay(cfg *Config, serverClient *Client, cache *Cache) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			replayCache(ctx, cfg, serverClient, cache)
		}
	}
}

func replayCache(ctx context.Context, cfg *Config, serverClient *Client, cache *Cache) {
	replayClient := newHTTPClient(cfg.ServerURL, cfg.ServerSecret, cfg.NodeID)

	reportFunc := func(deltas []UserTrafficDelta, timestamp int64) error {
		return reportTraffic(replayClient, cfg.NodeID, deltas, timestamp)
	}

	if err := cache.Replay(reportFunc); err != nil {
		slog.Warn("缓存重放完成，但有部分条目失败", "error", err)
	}
}

func reportTraffic(client *Client, nodeID string, deltas []UserTrafficDelta, timestamp int64) error {
	if len(deltas) == 0 {
		return nil
	}

	body, err := json.Marshal(map[string]interface{}{
		"node_id":   nodeID,
		"timestamp": timestamp,
		"users":     deltas,
	})
	if err != nil {
		return fmt.Errorf("序列化补报请求: %w", err)
	}

	resp, err := client.Post(context.Background(), "/api/v1/traffic/report", body, nil)
	if err != nil {
		return fmt.Errorf("补报请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("补报服务器返回 %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
