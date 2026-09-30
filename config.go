// config.go — 配置定义、yaml 加载与默认值
//
// 约定：
//   - 所有字段都是小写未导出，通过 getters 暴露（未来可增加校验）
//   - 配置文件缺失字段时使用默认值，不会报错退出
//   - NodeID / ServerSecret / HysteriaSecret 为空字符串时视为配置错误
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是整个客户端的配置树。
type Config struct {
	NodeID       string         `yaml:"node_id"`
	ServerURL    string         `yaml:"server_url"`
	ServerSecret string         `yaml:"server_secret"`

	Hysteria HysteriaConfig `yaml:"hysteria"`

	Intervals IntervalsConfig `yaml:"intervals"`

	Cache CacheConfig `yaml:"cache"`

	Log LogConfig `yaml:"log"`
}

// HysteriaConfig 是对接本地 Hysteria v2 进程的配置。
type HysteriaConfig struct {
	APIURL  string `yaml:"api_url"`
	Secret  string `yaml:"secret"`
}

// IntervalsConfig 定义各个定时任务的周期。
type IntervalsConfig struct {
	TrafficReport Duration `yaml:"traffic_report"`
	KickCheck     Duration `yaml:"kick_check"`
	Heartbeat     Duration `yaml:"heartbeat"`
}

// Duration 是 yaml.v3 无法直接反序列化 time.Duration 的 workaround：
// 读成 string 后再解析。支持 "10s"、"1m" 等 go 合法格式。
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	// 兼容两种写法：直接写成字符串 "10s"，或嵌套对象 {duration: "10s"}
	if value.Kind == yaml.ScalarNode {
		var s string
		if err := value.Decode(&s); err != nil {
			return err
		}
		d.Duration, _ = time.ParseDuration(s)
		return nil
	}
	// 对象形式，字段名可能是 duration / value / string 等，尝试 decode 成字符串再解析
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("invalid intervals duration: %w", err)
	}
	d.Duration, _ = time.ParseDuration(s)
	return nil
}

// CacheConfig 本地补报缓存配置。
type CacheConfig struct {
	Path       string `yaml:"path"`
	MaxEntries int    `yaml:"max_entries"`
}

// LogConfig 日志配置。
type LogConfig struct {
	Level string `yaml:"level"` // "debug" | "info" | "warn" | "error"
}

// Load 从指定路径读取 YAML 配置文件，缺失字段应用默认值。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := defaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// 覆盖默认值后的合法性检查
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node_id 不能为空")
	}
	if cfg.ServerURL == "" {
		return nil, fmt.Errorf("server_url 不能为空")
	}
	if cfg.ServerSecret == "" {
		return nil, fmt.Errorf("server_secret 不能为空")
	}
	if cfg.Hysteria.APIURL == "" {
		return nil, fmt.Errorf("hysteria.api_url 不能为空")
	}
	if cfg.Hysteria.Secret == "" {
		return nil, fmt.Errorf("hysteria.secret 不能为空")
	}
	if cfg.Cache.Path == "" {
		return nil, fmt.Errorf("cache.path 不能为空")
	}
	if cfg.Cache.MaxEntries <= 0 {
		cfg.Cache.MaxEntries = defaultConfig().Cache.MaxEntries
	}

	slog.Info("配置加载完成",
		"node_id", cfg.NodeID,
		"server", cfg.ServerURL,
		"traffic_interval", cfg.Intervals.TrafficReport.Duration,
		"kick_interval", cfg.Intervals.KickCheck.Duration,
		"heartbeat_interval", cfg.Intervals.Heartbeat.Duration,
		"cache", cfg.Cache.Path,
	)

	return cfg, nil
}

// defaultConfig 返回带有默认值的配置模板。
func defaultConfig() *Config {
	return &Config{
		ServerURL:     "http://127.0.0.1:8080",
		ServerSecret:  "",
		Hysteria: HysteriaConfig{
			APIURL: "http://127.0.0.1:9999",
			Secret: "",
		},
		Intervals: IntervalsConfig{
			TrafficReport: Duration{Duration: 10 * time.Second},
			KickCheck:     Duration{Duration: 15 * time.Second},
			Heartbeat:     Duration{Duration: 30 * time.Second},
		},
		Cache: CacheConfig{
			Path:       "/var/lib/hy-client/cache.json",
			MaxEntries: 10000,
		},
		Log: LogConfig{
			Level: "info",
		},
	}
}

// LogLevel 返回 slog 对应的 Level。
func (l LogConfig) LogLevel() slog.Level {
	switch l.Level {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
