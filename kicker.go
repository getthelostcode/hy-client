// kicker.go — 待踢列表拉取 + 调用 Hysteria /kick + ACK 服务器
//
// 闭环流程（默认 15s 周期）：
//   1. GET Server /api/v1/kick/list?node_id=xxx → 获取待踢列表
//   2. 调用 Hysteria POST /kick 断连
//   3. POST Server /api/v1/kick/ack 确认踢人完成
//
// 注意：
//   - Hysteria /kick 只是断连，客户端会自动重连
//   - 真正阻止用户需 Server 侧认证后端配合，本 Client 只做调用和 ACK
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Kicker 是踢人闭环处理器。
type Kicker struct {
	cfg          *Config
	hyClient     *HysteriaClient
	serverClient *Client
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// NewKicker 创建踢人处理器。
func NewKicker(cfg *Config, hyClient *HysteriaClient, serverClient *Client) *Kicker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Kicker{
		cfg:          cfg,
		hyClient:     hyClient,
		serverClient: serverClient,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start 启动踢人循环。
func (k *Kicker) Start() {
	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		ticker := time.NewTicker(k.cfg.Intervals.KickCheck.Duration)
		defer ticker.Stop()

		for {
			select {
			case <-k.ctx.Done():
				return
			case <-ticker.C:
				k.processKickList()
			}
		}
	}()
}

// Stop 停止踢人处理器。
func (k *Kicker) Stop() {
	k.cancel()
	k.wg.Wait()
}

// processKickList 执行一次待踢列表处理。
func (k *Kicker) processKickList() {
	// STEP 1: 从 Server 拉取待踢列表
	kickList, err := k.fetchKickList()
	if err != nil {
		slog.Error("获取待踢列表失败", "error", err)
		return
	}

	if len(kickList) == 0 {
		return
	}

	slog.Info("收到待踢列表", "count", len(kickList), "reasons", countReasons(kickList))

	// STEP 2: 调用 Hysteria /kick 踢人
	// 注意：即使部分用户踢不掉（Hysteria API 错误），也要继续踢其他用户
	userIDs := make([]string, 0, len(kickList))
	for _, k := range kickList {
		userIDs = append(userIDs, k.UserID)
	}

	// 批量踢：一次 /kick 调用传入所有 userIDs
	if err := k.hyClient.Kick(k.ctx, userIDs); err != nil {
		slog.Warn("调用 Hysteria /kick 部分失败", "error", err, "users", len(userIDs))
		// 不中断 ACK 流程，仍然ACK已踢的用户（Hysteria可能部分成功）
	}

	// STEP 3: ACK 服务器
	if err := k.ackServer(userIDs); err != nil {
		slog.Warn("ACK 服务器失败", "error", err)
	}
}

// fetchKickList 从 Server 获取待踢列表。
func (k *Kicker) fetchKickList() ([]KickItem, error) {
	path := fmt.Sprintf("/api/v1/kick/list?node_id=%s", url.QueryEscape(k.cfg.NodeID))
	resp, err := k.serverClient.Get(k.ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("获取踢人列表: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("踢人列表接口返回 %d: %s", resp.StatusCode, string(body))
	}

	var kickResponse kickListResponse
	if err := json.NewDecoder(resp.Body).Decode(&kickResponse); err != nil {
		return nil, fmt.Errorf("解析踢人列表: %w", err)
	}

	return kickResponse.Kick, nil
}

// kickListResponse 是 GET /api/v1/kick/list 的响应体。
type kickListResponse struct {
	Kick []KickItem `json:"kick"`
}

// KickItem 是待踢用户条目。
type KickItem struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason"`
}

// countReasons 返回每个 reason 类型的计数（用于日志）。

// ackServer 向 Server 确认踢人完成。
func (k *Kicker) ackServer(userIDs []string) error {
	if len(userIDs) == 0 {
		return nil
	}

	body, err := json.Marshal(kickAckRequest{
		NodeID:  k.cfg.NodeID,
		UserIDs: userIDs,
	})
	if err != nil {
		return fmt.Errorf("序列化 ACK 请求: %w", err)
	}

	resp, err := k.serverClient.Post(k.ctx, "/api/v1/kick/ack", body, nil)
	if err != nil {
		return fmt.Errorf("ACK 请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ACK 服务器返回 %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// kickAckRequest 是 POST /api/v1/kick/ack 的请求体。
type kickAckRequest struct {
	NodeID  string   `json:"node_id"`
	UserIDs []string `json:"user_ids"`
}

// 工具函数：统计 reason 分布。
func countReasons(items []KickItem) map[string]int {
	counts := make(map[string]int)
	for _, item := range items {
		counts[item.Reason]++
	}
	return counts
}
