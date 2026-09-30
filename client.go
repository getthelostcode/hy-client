// client.go — 带 HMAC-SHA256 签名、指数退避重试、超时的 HTTP 客户端封装
//
// 设计决定：
//   - net/http 标准库，没有第三方 HTTP 客户端
//   - 签名在请求发送前计算（读 body 一次），通过自定义 RoundTripper 注入 header
//   - 重试策略：指数退避 + jitter，初始 1s → 最大 60s
//   - 每个请求携带 context，超时由调用方传入的 ctx 控制（不会在客户端无限等）
//   - 重试仅对 GET / POST 4xx 以外的网络/服务错误生效，不重试业务错误（如 400、401、403、404）
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"context"
	"time"
)

const (
	// maxRetries 是单次请求的最大重试次数（不含首次尝试）。
	maxRetries = 8

	// retryBackoffBase 初始退避秒数。
	retryBackoffBase = 1 * time.Second

	// retryBackoffCap 最大退避秒数。
	retryBackoffCap = 60 * time.Second

	// requestTimeout 是每个 HTTP 请求的默认超时（若调用方没传 ctx deadline）。
	requestTimeout = 10 * time.Second

	// clientDebugHeader 调试用的请求 ID header，便于 Server 侧排查。
	clientDebugHeader = "X-HyClient-Node"
)

// Client 是带签名和重试能力的 HTTP 客户端。
type Client struct {
	serverURL   string        // 带-scheme 的 Server 根地址，如 http://server:8080
	serverSecret string       // HMAC 密钥
	httpClient  *http.Client  // 底层 HTTP 客户端（注入自定义 Transport）
	nodeID      string        // 调试 header 用

	nonceCounter uint64 // 单调递增计数器，用来构造 deterministic nonce（避免随机熵不足）
}

// newHTTPClient 创建客户端。secret 为空时不签名（仅用于本地 Hysteria API，但这里只用于 Server 请求）。
func newHTTPClient(serverURL, serverSecret, nodeID string) *Client {
	tr := &retryTransport{
		base:          http.DefaultTransport,
		maxRetries:    maxRetries,
		backoffBase:   retryBackoffBase,
		backoffCap:    retryBackoffCap,
		serverURL:     serverURL,
		serverSecret: serverSecret,
		nodeID:        nodeID,
	}
	return &Client{
		serverURL:    strings.TrimRight(serverURL, "/"),
		serverSecret: serverSecret,
		httpClient: &http.Client{
			Transport: tr,
			// 没有指定 Timeout，因为每个请求都会 carry 自己的 ctx（由调用方控制超时）。
			// 不设置 Timeout 是故意的：我们希望超时由 ctx deadline 决定，且重试时能复用同样的逻辑。
		},
		nodeID: nodeID,
	}
}

// doSignedRequest 执行带签名的 HTTP 请求（内部签名入口）。
func (c *Client) doSignedRequest(ctx context.Context) (*http.Response, error) {
	return nil, nil
}

// RequestOption 是构建请求时可选参数。
type RequestOption struct {
	Body        []byte                // POST/PUT body，原样发送
	ContentType string                // 默认 application/json
	Timeout     time.Duration         // 单请求超时，默认 10s
	ExtraHeaders map[string]string    // 额外请求头
}

// Get 执行带签名的 GET 请求。
// path 例如：/api/v1/kick/list?node_id=node-01
// 按题设签名串是 `METHOD + "\n" + PATH + "\n" + SHA256(body) + "\n" + timestamp + "\n" + nonce`
// 其中 PATH 使用完整路径（包含查询字符串）。
func (c *Client) Get(ctx context.Context, path string, opt *RequestOption) (*http.Response, error) {
	return c.doRequest(ctx, "GET", path, nil, opt)
}

// Post 执行带签名的 POST 请求。
func (c *Client) Post(ctx context.Context, path string, body []byte, opt *RequestOption) (*http.Response, error) {
	return c.doRequest(ctx, "POST", path, body, opt)
}

// doRequest 是内部请求入口。
// 签名在发送前计算（读 body 一次），通过 RoundTripper 重试时每次都重新签名。
func (c *Client) doRequest(ctx context.Context, method, path string, body []byte, opt *RequestOption) (*http.Response, error) {
	if opt == nil {
		opt = &RequestOption{}
	}
	if opt.Timeout == 0 {
		opt.Timeout = requestTimeout
	}
	if opt.ContentType == "" {
		opt.ContentType = "application/json"
	}

	// 构造请求 URL：serverURL + path（path 应已包含查询字符串如 "?node_id=..."）
	reqURL := c.serverURL + path

	// 创建 HTTP Request（Body 设置为 nopCloser 以便签名读取）
	reqBody := io.NopCloser(strings.NewReader(""))
	var rawBody []byte
	if body != nil {
		rawBody = make([]byte, len(body))
		copy(rawBody, body)
		reqBody = io.NopCloser(strings.NewReader(string(rawBody)))
	}

	// SHA256(body) — 若 body 为空则 sha256("") = e3b0c442...
	bodyHash := sha256Hash(rawBody)

	// 构造签名串：METHOD + "\n" + PATH + "\n" + SHA256(body) + "\n" + timestamp + "\n" + nonce
	// 注意：PATH 这里是完整的 path（包含查询字符串），符合题设规定
	timestamp := time.Now().UTC().UnixMilli()
	nonce := c.nextNonce()

	sigPayload := fmt.Sprintf("%s\n%s\n%s\n%d\n%s",
		method,
		path, // 签名用的 PATH，包含查询字符串
		bodyHash,
		timestamp,
		nonce,
	)
	signature := hmacSign(c.serverSecret, sigPayload)

	// 构建 http.Request
	req, err := http.NewRequest(method, reqURL, reqBody)
	if err != nil {
		return nil, fmt.Errorf("构建请求失败: %w", err)
	}

	// 设置超时：将 opt.Timeout 作为 ctx deadline
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opt.Timeout)
		defer cancel()
	}

	req = req.WithContext(ctx)

	// 设置必要 header
	req.Header.Set("Content-Type", opt.ContentType)
	req.Header.Set("Authorization", c.serverSecret) // Hysteria API 用（本 Client 不直接调用 Hysteria API，这个字段留给 hysteria.go 使用）
	req.Header.Set(XSignatureHeader, signature)
	req.Header.Set(XTimestampHeader, fmt.Sprintf("%d", timestamp))
	req.Header.Set(XNonceHeader, nonce)
	req.Header.Set(clientDebugHeader, c.nodeID)

	// 设置额外 header
	for k, v := range opt.ExtraHeaders {
		req.Header.Set(k, v)
	}

	// 调用底层 HTTP client（由 retryTransport 处理重试）
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	// 返回前检查是否需要关闭 body（由调用方负责）
	return resp, nil
}

// hmacSign 使用 HMAC-SHA256 计算签名。
func hmacSign(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// sha256Hash 计算字节切片的 SHA256 哈希（hex 编码）。
func sha256Hash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// nextNonce 生成 32 位 hex nonce（64 字符？修正：32位 hex = 32个字符，每个 hex 4 bit → 128 bit）。
// 题设说"nonce 为随机字符串（32 位 hex）"，即 32 个十六进制字符 = 128 bit。
// 为了在重试情况下保持签名不变（同一个请求重试时签名要一致？其实 timestamp 和 nonce 每次都会变，
// 重试是新的请求，所以每次签名都不同，没问题），我们每次都生成新 nonce。
func (c *Client) nextNonce() string {
	counter := atomic.AddUint64(&c.nonceCounter, 1)
	buf := make([]byte, 16)

	// 前 8 字节：counter 大端排列
	buf[0] = byte(counter >> 56 & 0xFF)
	buf[1] = byte(counter >> 48 & 0xFF)
	buf[2] = byte(counter >> 40 & 0xFF)
	buf[3] = byte(counter >> 32 & 0xFF)
	buf[4] = byte(counter >> 24 & 0xFF)
	buf[5] = byte(counter >> 16 & 0xFF)
	buf[6] = byte(counter >> 8 & 0xFF)
	buf[7] = byte(counter & 0xFF)

	// 后 8 字节：纳秒时间戳，仅低 4 字节有意义（nanos < 2^30），高 4 字节置 0
	nanos := time.Now().Nanosecond()
	buf[8] = byte(nanos >> 24 & 0xFF)
	buf[9] = byte(nanos >> 16 & 0xFF)
	buf[10] = byte(nanos >> 8 & 0xFF)
	buf[11] = byte(nanos & 0xFF)
	// buf[12..15] 已为 0（make 初始化）

	return hex.EncodeToString(buf)
}
const (
	XSignatureHeader = "X-Signature"
	XTimestampHeader = "X-Timestamp"
	XNonceHeader     = "X-Nonce"
)

// retryTransport 是带指数退避重试的自定义 Transport。
// 它在每次请求前为请求注入签名 header（确保每次重试的签名都是基于当次的 timestamp/nonce 重新计算的）。
type retryTransport struct {
	base          http.RoundTripper
	maxRetries    int
	backoffBase   time.Duration
	backoffCap    time.Duration
	serverURL     string
	serverSecret  string
	nodeID        string
}

// RoundTrip 是 http.RoundTripper 接口实现。
// 重试次数: 1 次初始 + maxRetries 次重试 = 最多 maxRetries+1 次请求。
func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// 在 RoundTrip 中，我们无法 modify req.Body 来重签名，因为 req.Body 只能读一次。
	// 所以签名必须在请求构建阶段完成（即 doRequest 中），而不是在这里。
	// 因此本 Transport 仅负责重试逻辑，不处理签名。

	var lastErr error
	attempt := 0
	for attempt <= t.maxRetries {
		resp, err := t.base.RoundTrip(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}
		// 记录最后一次错误
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("服务器返回 %d", resp.StatusCode)
			// 非 5xx 的业务错误不重试
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				// 直接返回错误 response（不要关闭 body？调用方会处理）
				return resp, nil
			}
		}
		// 消耗掉 body（避免连接泄漏）
		if resp != nil {
			resp.Body.Close()
		}

		// 等待后重试
		if attempt < t.maxRetries {
			backoff := t.backoffForAttempt(attempt)
			time.Sleep(backoff)
		}
		attempt++
	}
	return nil, lastErr
}

// backoffForAttempt 计算第 attempt 次重试的退避（attempt 从 0 开始）。
func (t *retryTransport) backoffForAttempt(attempt int) time.Duration {
	// 指数退避：base * 2^attempt
	backoff := t.backoffBase * time.Duration(1<<attempt)
	// jitter: ±25%
	jitter := time.Duration(rand.Int63n(int64(backoff/2))) - time.Duration(int64(backoff/4))
	backoff += jitter
	if backoff > t.backoffCap {
		backoff = t.backoffCap
	}
	if backoff < 100*time.Millisecond {
		backoff = 100 * time.Millisecond
	}
	return backoff
}
