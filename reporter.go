// reporter.go — 流量增量计算、上报、补报触发
//
// 职责：
//   1. 每隔 intervals.traffic_report 秒调用 Hysteria GET /traffic 获取累计值
//   2. 与上次快照对比，计算每个用户的增量（tx/rx）
//   3. 遇到计数器重置（cur < last）时直接上报 cur 值
//   4. 把增量 POST 到 Server /api/v1/traffic/report
//   5. 上报失败 → 写入本地缓存，等待补报 goroutine 重放
//   6. 上报成功 → 更新快照，并删除本次对应的缓存条目（由补报协程处理）
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// Reporter 是流量采集与上报的核心组件。
type Reporter struct {
	cfg          *Config
	hyClient     *HysteriaClient
	serverClient *Client
	cache        *Cache
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup

	snapshot      TrafficResponse
	snapshotMutex sync.Mutex
}

// NewReporter 创建流量上报器。
func NewReporter(cfg *Config, hyClient *HysteriaClient, serverClient *Client, cache *Cache) *Reporter {
	ctx, cancel := context.WithCancel(context.Background())
	return &Reporter{
		cfg:          cfg,
		hyClient:     hyClient,
		serverClient: serverClient,
		cache:        cache,
		ctx:          ctx,
		cancel:       cancel,
		snapshot:     make(TrafficResponse),
	}
}

// Start 启动流量采集循环（作为 goroutine 运行）。
func (r *Reporter) Start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.cfg.Intervals.TrafficReport.Duration)
		defer ticker.Stop()

		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				r.collectAndReport()
			}
		}
	}()
}

// Stop 停止流量上报器。
func (r *Reporter) Stop() {
	r.cancel()
	r.wg.Wait()
}

// collectAndReport 执行一次流量采集与上报。
func (r *Reporter) collectAndReport() {
	r.snapshotMutex.Lock()
	last := r.snapshot
	r.snapshotMutex.Unlock()

	cur, err := r.hyClient.GetTraffic(r.ctx, false)
	if err != nil {
		slog.Error("获取 Hysteria 流量失败", "error", err)
		return
	}

	delta := r.calculateDelta(last, cur)

	if len(delta) > 0 {
		timestamp := time.Now().Unix()
		if err := r.reportToServer(delta, timestamp); err != nil {
			slog.Warn("流量上报失败，加入补报缓存", "error", err, "users", len(delta))
			r.cache.Append(delta, timestamp)
		} else {
			slog.Info("流量上报成功", "users", len(delta))
		}
	}

	r.snapshotMutex.Lock()
	r.snapshot = cur
	r.snapshotMutex.Unlock()
}

// calculateDelta 计算两个累计快照之间的增量。
// 关键逻辑：
//   - cur.tx < last.tx 或 cur.rx < last.rx 说明 Hysteria 重启/清零了
//     → 直接上报 cur(tx, rx) 作为本次增量（不能用 0，上报 0 意味流量丢失）
//   - 否则用 cur - last 计算增量
//   - 不存在于 last 的新用户，直接用 cur 值
func (r *Reporter) calculateDelta(last, cur TrafficResponse) []UserTrafficDelta {
	var deltas []UserTrafficDelta

	for userID, curVal := range cur {
		lastVal, exists := last[userID]
		if !exists {
			deltas = append(deltas, UserTrafficDelta{
				UserID:   userID,
				Upload:   curVal.Upload,
				Download: curVal.Download,
			})
			continue
		}

		uploadDelta := curVal.Upload - lastVal.Upload
		downloadDelta := curVal.Download - lastVal.Download

		if curVal.Upload < lastVal.Upload || curVal.Download < lastVal.Download {
			slog.Warn("检测到流量计数器重置（可能 Hysteria 重启）",
				"user", userID,
				"last_upload", lastVal.Upload,
				"cur_upload", curVal.Upload,
				"last_download", lastVal.Download,
				"cur_download", curVal.Download,
			)
			deltas = append(deltas, UserTrafficDelta{
				UserID:   userID,
				Upload:   curVal.Upload,
				Download: curVal.Download,
			})
		} else if uploadDelta > 0 || downloadDelta > 0 {
			deltas = append(deltas, UserTrafficDelta{
				UserID:   userID,
				Upload:   uploadDelta,
				Download: downloadDelta,
			})
		}
	}

	return deltas
}

// UserTrafficDelta 是单个用户的流量增量（上报给 Server 的单位）。
type UserTrafficDelta struct {
	UserID   string `json:"user_id"`
	Upload   uint64 `json:"upload"`
	Download uint64 `json:"download"`
}

// trafficReportRequest 是 POST /api/v1/traffic/report 的请求体。
type trafficReportRequest struct {
	NodeID    string             `json:"node_id"`
	Timestamp int64              `json:"timestamp"`
	Users     []UserTrafficDelta `json:"users"`
}

// reportToServer 把流量增量上报到 Server。
func (r *Reporter) reportToServer(deltas []UserTrafficDelta, timestamp int64) error {
	if len(deltas) == 0 {
		return nil
	}

	body, err := json.Marshal(trafficReportRequest{
		NodeID:    r.cfg.NodeID,
		Timestamp: timestamp,
		Users:     deltas,
	})
	if err != nil {
		return fmt.Errorf("序列化上报请求: %w", err)
	}

	resp, err := r.serverClient.Post(r.ctx, "/api/v1/traffic/report", body, nil)
	if err != nil {
		return fmt.Errorf("上报请求失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("上报服务器返回 %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// LoadSnapshot 从缓存文件加载上次快照（进程重启恢复）。
func (r *Reporter) LoadSnapshot() error {
	data, err := os.ReadFile(r.cfg.Cache.Path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("缓存文件不存在，使用空快照")
			r.snapshotMutex.Lock()
			r.snapshot = make(TrafficResponse)
			r.snapshotMutex.Unlock()
			return nil
		}
		return fmt.Errorf("读取缓存文件: %w", err)
	}

	var cacheData cacheFileFormat
	if err := json.Unmarshal(data, &cacheData); err != nil {
		slog.Warn("缓存文件格式异常，无法加载快照", "error", err)
		r.snapshotMutex.Lock()
		r.snapshot = make(TrafficResponse)
		r.snapshotMutex.Unlock()
		return nil
	}

	r.snapshotMutex.Lock()
	r.snapshot = cacheData.Snapshot
	r.snapshotMutex.Unlock()

	slog.Info("从缓存恢复流量快照", "users", len(r.snapshot))
	return nil
}

// cacheFileFormat 是缓存文件的格式（用于加载 snapshot）。
type cacheFileFormat struct {
	Snapshot TrafficResponse `json:"snapshot"`
	Entries  []CacheEntry    `json:"entries"`
}
