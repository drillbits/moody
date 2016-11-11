package moody

import (
	"context"
	"os"

	"github.com/BurntSushi/toml"
)

type private struct{}

var cfgKey private

// Config is a configuration for Nejireco Pub/Sub.
type Config struct {
	RedisURI string     `toml:"redis_uri"`
	GCP      *GCPConfig `toml:"gcp"`
}

// GCPConfig is a configuration for Google Cloud Platform.
type GCPConfig struct {
	ProjectID          string `toml:"project_id"`
	ServiceAccountFile string `toml:"service_account_file"`
}

// DefaultConfig returns default configuration.
func DefaultConfig() *Config {
	return &Config{
		RedisURI: "redis://127.0.0.1:6379",
		GCP: &GCPConfig{
			ProjectID:          "",
			ServiceAccountFile: "",
		},
	}
}

// NewConfig creates a new configuration.
func NewConfig(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	if configPath == "" {
		return cfg, nil
	}

	// if configPath does not exist, return default configuration
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return cfg, nil
	}

	_, err := toml.DecodeFile(configPath, &cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// NewContext creates a new context.
func NewContext(ctx context.Context, cfg *Config) context.Context {
	return context.WithValue(ctx, cfgKey, cfg)
}

// ConfigFromContext returns a configuration in context or default one.
func ConfigFromContext(ctx context.Context) *Config {
	if cfg, ok := ctx.Value(cfgKey).(*Config); ok && cfg != nil {
		return cfg
	}
	return DefaultConfig()
}
