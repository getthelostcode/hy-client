// hysteria.go — Hysteria v2 本地 API 封装
//
// 封装本地 Hysteria v2 监听的 HTTP API（默认 http://127.0.0.1:9999）。
// 所有请求带 Authorization: *** header。
// 本文件只调用本地 API，不经过 Server 的 HTTP 客户端签名逻辑。
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HysteriaClient 是对接本地 Hysteria v2 进程的客户端。
type HysteriaClient struct {
	apiURL  string // 如 http://127.0.0.1:9999
	secret  string // Authorization header 值
	http    *http.Client // 不带重试的简单客户端，超时由调用方 ctx 控制
}

// NewHysteriaClient 创建本地 Hysteria API 客户端。
func NewHysteriaClient(apiURL, secret string) *HysteriaClient {
	return &HysteriaClient{
		apiURL: strings.TrimRight(apiURL, "/"),
		secret: secret,
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: 3 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout:   3 * time.Second,
				ResponseHeaderTimeout: 5 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
			Timeout: 5 * time.Second,
		},
	}
}

// TrafficResult 是 /traffic 接口的单个用户流量数据。
type TrafficResult struct {
	UserIDs map[string]struct {
		Upload uint64 `json:"tx"` // 累计上传字节
		Download uint64 `json:"rx"` // 累计下载字节
	} `json:"userids"`
}

// TrafficResponse 是 /traffic 接口返回的完整结构。
type TrafficResponse map[string]struct {
	Upload   uint64 `json:"tx"`
	Download uint64 `json:"rx"`
}

// GetTraffic 调用 GET /traffic 获取累计流量。
// 返回值是 userid → {tx, rx} 的映射。
// clear 参数决定是否在获取后清零（默认 false，本 Client 不使用 clear=1）。
func (h *HysteriaClient) GetTraffic(ctx interface{}, clear bool) (TrafficResponse, error) {
	// 真实 ctx 是 context.Context
	q := url.Values{}
	if clear {
		q.Set("clear", "1")
	}
	query := q.Encode()
	path := "/traffic"
	if query != "" {
		path += "?" + query
	}

	req, err := http.NewRequest("GET", h.apiURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求: %w", err)
	}
	req.Header.Set("Authorization", h.secret)

	resp, err := h.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("调用 Hysteria /traffic: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Hysteria /traffic 返回 %d: %s", resp.StatusCode, string(body))
	}

	var result TrafficResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("解析 /traffic 响应: %w", err)
	}
	return result, nil
}

// OnlineResult 是 /online 接口的返回。
type OnlineResult map[string]int // userid → device_count

// GetOnline 调用 GET /online 获取当前在线用户。
func (h *HysteriaClient) GetOnline(ctx interface{}) (OnlineResult, error) {
	req, err := http.NewRequest("GET", h.apiURL+"/online", nil)
	if err != nil {
		return nil, fmt.Errorf("创建请求: %w", err)
	}
	req.Header.Set("Authorization", h.secret)

	resp, err := h.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("调用 Hysteria /online: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Hysteria /online 返回 %d: %s", resp.StatusCode, string(body))
	}

	var result OnlineResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("解析 /online 响应: %w", err)
	}
	return result, nil
}

// KickPayload 是 POST /kick 的请求体。
type KickPayload struct {
	UserIDs []string `json:"user_ids"`
}

// Kick 调用 POST /kick 踢指定用户。
// 注意：Hysteria 的 /kick 只是断连，客户端会自动重连。
// 真正阻止用户必须依赖 Server 侧认证后端。本 Client 只负责调用 /kick。
func (h *HysteriaClient) Kick(ctx interface{}, userIDs []string) error {
	body, err := json.Marshal(KickPayload{UserIDs: userIDs})
	if err != nil {
		return fmt.Errorf("序列化 kick body: %w", err)
	}

	req, err := http.NewRequest("POST", h.apiURL+"/kick", strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("创建请求: %w", err)
	}
	req.Header.Set("Authorization", h.secret)
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.http.Do(req)
	if err != nil {
		return fmt.Errorf("调用 Hysteria /kick: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Hysteria /kick 返回 %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

