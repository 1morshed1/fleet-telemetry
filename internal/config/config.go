// Package config provides tiny helpers for reading configuration from
// environment variables with sane defaults. Twelve-factor style: every service
// is configured by the environment, so the same binary runs locally and in a
// container without code changes.
package config

import (
	"os"
	"strconv"
	"time"
)

// String returns the env var, or def if unset/empty.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Int returns the env var parsed as int, or def if unset/invalid.
func Int(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// Duration returns the env var parsed as a Go duration (e.g. "100ms"), or def.
func Duration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
