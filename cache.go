// cache.go — 本地 JSON 缓存（原子写入、按序重放）
//
// 设计决定：
//   - 纯 JSON 文件，不引入 SQLite（符合题目要求）
//   - 原子写入：先写 .tmp 文件，fsync، 再 os.Rename 覆盖原文件
//   - 进程被 kill -9 时：可能是 .tmp 文件残留，但原 cache.json 不会损坏
//   - 重放按 timestamp 升序排序后依次重试，成功后删除该条目
//   - 缓存最大条目数限制，超出则最老的条目被淘汰
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
)

// Cache 是本地补报缓存管理器。
type Cache struct {
	path       string
	maxEntries int
	mu         sync.Mutex // 保护 entries 的并发访问

	// entries 是待补报的流量数据条目列表。
	// 每条记录包含增量数据和采集时间戳。
	entries []CacheEntry
}

// CacheEntry 是缓存中的一条待补报记录。
type CacheEntry struct {
	Users    []UserTrafficDelta `json:"users"`    // 流量增量
	Timestamp int64             `json:"timestamp"` // 采集时的 Unix 秒
	RetryCount int               `json:"retry_count"` // 重试次数（可选，用于调试）
}

// NewCache 创建缓存管理器，并从文件加载已有条目。
func NewCache(path string, maxEntries int) (*Cache, error) {
	c := &Cache{
		path:       path,
		maxEntries: maxEntries,
		entries:    make([]CacheEntry, 0),
	}

	if err := c.load(); err != nil {
		// 文件不存在是正常情况（首次运行）
		if os.IsNotExist(err) {
			slog.Info("缓存文件不存在，开始空缓存")
			return c, nil
		}
		return nil, fmt.Errorf("加载缓存: %w", err)
	}

	slog.Info("缓存加载完成", "entries", len(c.entries), "path", path)
	return c, nil
}

// load 从缓存文件加载条目。
func (c *Cache) load() error {
	data, err := os.ReadFile(c.path)
	if err != nil {
		return err
	}

	var entries []CacheEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("解析缓存 JSON: %w", err)
	}

	c.mu.Lock()
	c.entries = entries
	c.mu.Unlock()
	return nil
}

// Append 追加一条待补报记录。
// 如果缓存已满，淘汰最老的条目。
func (c *Cache) Append(deltas []UserTrafficDelta, timestamp int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := CacheEntry{
		Users:      deltas,
		Timestamp:  timestamp,
		RetryCount: 0,
	}
	c.entries = append(c.entries, entry)

	// 超出最大条目数：移除最老的条目（FIFO）
	for len(c.entries) > c.maxEntries {
		c.entries = c.entries[1:]
	}

	// 原子写入文件
	if err := c.save(); err != nil {
		slog.Error("缓存写入失败", "error", err)
	}
}

// save 原子写入缓存文件。
// 写法：先写入 .tmp 文件，fsync，最后 os.Rename 替换原文件。
// 这样可以保证 cache.json 始终是完整的 JSON（不会出现写了一半的文件）。
func (c *Cache) save() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 对 entries 按 timestamp 排序后再写入文件（确保重放顺序）
	sort.Slice(c.entries, func(i, j int) bool {
		return c.entries[i].Timestamp < c.entries[j].Timestamp
	})

	data, err := json.MarshalIndent(c.entries, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化缓存: %w", err)
	}

	tmpPath := c.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("写入临时缓存文件: %w", err)
	}

	// fsync 保证数据落地
	f, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("打开临时文件: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync 临时文件: %w", err)
	}
	f.Close()

	// 原子替换
	if err := os.Rename(tmpPath, c.path); err != nil {
		return fmt.Errorf("替换缓存文件: %w", err)
	}

	return nil
}

// Replay 从缓存中读取所有条目，按 timestamp 顺序重放（上报）到服务器。
// 重放成功的条目会从缓存中删除。
// 此函数由补报 goroutine 调用。
func (c *Cache) Replay(reportFunc func([]UserTrafficDelta, int64) error) error {
	c.mu.Lock()
	if len(c.entries) == 0 {
		c.mu.Unlock()
		return nil
	}

	// 复制一份待重放的条目（避免锁住太久）
	entries := make([]CacheEntry, len(c.entries))
	copy(entries, c.entries)
	c.mu.Unlock()

	// 按 timestamp 排序（确保按时间顺序重放）
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Timestamp < entries[j].Timestamp
	})

	var kept []CacheEntry
	var lastErr error

	for i, entry := range entries {
		slog.Info("重放缓存条目", "index", i, "users", len(entry.Users), "timestamp", entry.Timestamp)

		if err := reportFunc(entry.Users, entry.Timestamp); err != nil {
			slog.Warn("缓存条目重放失败", "error", err, "timestamp", entry.Timestamp)
			entry.RetryCount++
			kept = append(kept, entry)
			lastErr = err
			continue
		}

		slog.Info("缓存条目重放成功", "timestamp", entry.Timestamp)
		// 成功：不加入 kept 列表，即从缓存删除
	}

	// 写回剩余的条目
	c.mu.Lock()
	c.entries = kept
	c.mu.Unlock()

	if len(kept) > 0 {
		if err := c.save(); err != nil {
			slog.Error("缓存保存失败", "error", err)
			return err
		}
	} else {
		// 清空缓存：删除文件
		if err := os.Remove(c.path); err != nil && !os.IsNotExist(err) {
			slog.Warn("删除空缓存文件失败", "error", err)
		}
	}

	return lastErr
}

// Len 返回缓存条目数。
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Flush 清空缓存（用于测试或灾难恢复）。
func (c *Cache) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = nil
	return os.Remove(c.path)
}
