// heartbeat.go — 在线用户数采集 + Server 心跳上报
//
// 默认 30s 周期：
//   1. GET Hysteria /online 获取在线用户字典
//   2. 统计在线用户数（字典 key 的数量 = 活跃用户数，不是 device 总数）
//   3. POST Server /api/v1/node/heartbeat 上报
//
// 注意：题目中 online 返回的是 {"userid": device_count, ...}
//      心跳上报的 online_users 应该是"在线用户数"——即有多少个不同的 userid 在线
//      而不是 device_count 的总和（因为一个用户可能有多个设备）
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Heartbeat 是心跳发送器。
type Heartbeat struct {
	cfg          *Config
	hyClient     *HysteriaClient
	serverClient *Client
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup

	version string // Client 版本号，体现在心跳里
}

// NewHeartbeat 创建心跳发送器。
func NewHeartbeat(cfg *Config, hyClient *HysteriaClient, serverClient *Client, version string) *Heartbeat {
	ctx, cancel := context.WithCancel(context.Background())
	return &Heartbeat{
		cfg:          cfg,
		hyClient:     hyClient,
		serverClient: serverClient,
		ctx:          ctx,
		cancel:       cancel,
		version:      version,
	}
}

// Start 启动心跳循环。
func (h *Heartbeat) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(h.cfg.Intervals.Heartbeat.Duration)
		defer ticker.Stop()

		for {
			select {
			case <-h.ctx.Done():
				return
			case <-ticker.C:
				h.sendHeartbeat()
			}
		}
	}()
}

// Stop 停止心跳。
func (h *Heartbeat) Stop() {
	h.cancel()
	h.wg.Wait()
}

// sendHeartbeat 执行一次心跳上报。
func (h *Heartbeat) sendHeartbeat() {
	// STEP 1: 获取在线用户数（从 Hysteria /online）
	online, err := h.hyClient.GetOnline(h.ctx)
	if err != nil {
		slog.Error("获取在线用户列表失败", "error", err)
		// 使用上次缓存的在线数？没有缓存，直接上报 0（避免卡住心跳）
		h.reportHeartbeat(0)
		return
	}

	// 在线用户数 = 有多少个不同的 userid 在线（字典的 key 数）
	onlineUsers := len(online)
	slog.Debug("在线用户统计", "online_users", onlineUsers)

	// STEP 2: 上报心跳到 Server
	if err := h.reportHeartbeat(onlineUsers); err != nil {
		slog.Warn("心跳上报失败", "error", err)
	}
}

// reportHeartbeat 向 Server 发送心跳。
func (h *Heartbeat) reportHeartbeat(onlineUsers int) error {
	body, err := json.Marshal(heartbeatRequest{
		NodeID:       h.cfg.NodeID,
		Version:      h.version,
		OnlineUsers:  onlineUsers,
	})
	if err != nil {
		return fmt.Errorf("序列化心跳请求: %w", err)
	}

	resp, err := h.serverClient.Post(h.ctx, "/api/v1/node/heartbeat", body, nil)
	if err != nil {
		return fmt.Errorf("心跳请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("心跳服务器返回 %d: %s", resp.StatusCode, string(respBody))
	}

	// Server 可能返回建议的 interval（用于动态调整上报周期）
	// 目前不实现动态调整，记录日志即可
	slog.Debug("心跳上报成功")
	return nil
}

// heartbeatRequest 是 POST /api/v1/node/heartbeat 的请求体。
type heartbeatRequest struct {
	NodeID      string `json:"node_id"`
	Version     string `json:"version"`
	OnlineUsers int    `json:"online_users"`
}
