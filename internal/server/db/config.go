package db

import "time"

type Config struct {
	Dialect              string        `conf:"dialect" yaml:"dialect" json:"dialect"`
	DSN                  string        `conf:"dsn" yaml:"dsn" json:"dsn"`
	Debug                bool          `conf:"debug" yaml:"debug" json:"debug"`
	MaxOpenConns         int           `conf:"max_open_conns" yaml:"max_open_conns" json:"max_open_conns"`
	MaxIdleConns         int           `conf:"max_idle_conns" yaml:"max_idle_conns" json:"max_idle_conns"`
	ConnMaxLifetime      time.Duration `conf:"conn_max_lifetime" yaml:"conn_max_lifetime" json:"conn_max_lifetime"`
	ConnMaxIdleTime      time.Duration `conf:"conn_max_idle_time" yaml:"conn_max_idle_time" json:"conn_max_idle_time"`
	DisableSQLiteAutoWAL bool          `conf:"disable_sqlite_auto_wal" yaml:"disable_sqlite_auto_wal" json:"disable_sqlite_auto_wal"`
	DisableAutoMigration bool          `conf:"disable_auto_migration" yaml:"disable_auto_migration" json:"disable_auto_migration"`

	// TxWatch instruments the driver layer and reports transactions that stay
	// open, fail to commit, or take longer than TxWatchThreshold. It is
	// observation only; see instrumentation.go for why the holder of a SQLite
	// write lock is otherwise invisible.
	TxWatchEnabled   bool          `conf:"tx_watch_enabled" yaml:"tx_watch_enabled" json:"tx_watch_enabled"`
	TxWatchThreshold time.Duration `conf:"tx_watch_threshold" yaml:"tx_watch_threshold" json:"tx_watch_threshold"`
	TxWatchInterval  time.Duration `conf:"tx_watch_interval" yaml:"tx_watch_interval" json:"tx_watch_interval"`
	TxWatchHistory   int           `conf:"tx_watch_history" yaml:"tx_watch_history" json:"tx_watch_history"`

	ReadReplica ReadReplicaConfig `conf:"read_replica" yaml:"read_replica" json:"read_replica"`
}

type ReadReplicaConfig struct {
	DSN          string `conf:"read_dsn" yaml:"read_dsn" json:"read_dsn"`
	MaxOpenConns int    `conf:"read_max_open_conns" yaml:"read_max_open_conns" json:"read_max_open_conns"`
	MaxIdleConns int    `conf:"read_max_idle_conns" yaml:"read_max_idle_conns" json:"read_max_idle_conns"`
}
