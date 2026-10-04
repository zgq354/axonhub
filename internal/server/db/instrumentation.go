package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/looplj/axonhub/internal/log"
	sqliteshim "github.com/looplj/axonhub/internal/pkg/sqlite"
)

// SQLite has a single writer: a transaction (or even a single statement) that
// keeps the write lock longer than the busy timeout makes every other writer
// fail with SQLITE_BUSY. The holder is frequently invisible in a goroutine
// dump, because the lock belongs to a connection whose caller is gone - there is
// no stack left to look at. This file instruments the database/sql driver layer
// so the holder can be read out of the running process instead of guessed from
// `/proc/locks` and periodic dumps:
//
//  1. every transaction and every statement is timed; anything slower than the
//     threshold is logged with its caller and the statement involved;
//  2. a connection handed back to the pool (or closed) while a transaction is
//     still open is reported - that is the shape of a leaked-connection bug;
//  3. the counters and the recent events are served over HTTP.
//
// The wrapper is observation only. It never commits, rolls back, retries or
// replaces a statement on behalf of the application, so turning it on cannot
// hide the behaviour it exists to measure.

const (
	defaultTxWatchThreshold = time.Second
	defaultTxWatchInterval  = 10 * time.Second
	defaultTxWatchHistory   = 20
	txWatchMaxStatements    = 3
	txWatchMaxStatementLen  = 160
	txWatchMaxCallsiteLen   = 160
	txWatchMaxStack         = 4

	instrumentedSQLiteDriverName = "axonhub-sqlite3"
	instrumentedPGXDriverName    = "axonhub-pgx"
	instrumentedMySQLDriverName  = "axonhub-mysql"
)

// TxEvent is one recorded slow or failed database operation.
type TxEvent struct {
	ID         int64     `json:"id"`
	Kind       string    `json:"kind"`
	Dialect    string    `json:"dialect,omitempty"`
	Callsite   string    `json:"callsite,omitempty"`
	Statement  string    `json:"statement,omitempty"`
	Statements []string  `json:"statements,omitempty"`
	Stack      []string  `json:"stack,omitempty"`
	DurationMS int64     `json:"duration_ms"`
	AgeMS      int64     `json:"age_ms,omitempty"`
	At         time.Time `json:"at"`
	Error      string    `json:"error,omitempty"`
}

// InstrumentationCounters are the lifetime totals since process start.
type InstrumentationCounters struct {
	TransactionsBegun      int64 `json:"transactions_begun"`
	TransactionsCommitted  int64 `json:"transactions_committed"`
	CommitsFailed          int64 `json:"commits_failed"`
	Rollbacks              int64 `json:"rollbacks"`
	RollbacksFailed        int64 `json:"rollbacks_failed"`
	SlowTransactions       int64 `json:"slow_transactions"`
	SlowStatements         int64 `json:"slow_statements"`
	PoolReturnsWithOpenTx  int64 `json:"pool_returns_with_open_tx"`
	LongTransactionReports int64 `json:"long_transaction_reports"`
}

// InstrumentationReport is served by GET /admin/system/db-instrumentation.
type InstrumentationReport struct {
	Enabled          bool                    `json:"enabled"`
	ThresholdMS      int64                   `json:"threshold_ms"`
	WatchIntervalMS  int64                   `json:"watch_interval_ms"`
	Counters         InstrumentationCounters `json:"counters"`
	OpenTransactions []TxEvent               `json:"open_transactions"`
	RecentEvents     []TxEvent               `json:"recent_events"`
}

// watchedTx tracks one transaction from Begin to Commit/Rollback.
type watchedTx struct {
	id       int64
	dialect  string
	callsite string
	stack    []string
	started  time.Time

	mu     sync.Mutex
	stmts  []string
	closed bool
	// suspect is set when COMMIT failed: database/sql considers the transaction
	// finished as soon as Commit returns, so the connection can go back to the
	// pool while the driver still holds it. Keeping the record open lets the
	// pool-return check report exactly that connection.
	suspect string
}

func (t *watchedTx) note(query string) {
	if t == nil || query == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.stmts) < txWatchMaxStatements {
		t.stmts = append(t.stmts, truncate(query, txWatchMaxStatementLen))
	}
}

func (t *watchedTx) event(kind, err string) TxEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return TxEvent{
		ID:         t.id,
		Kind:       kind,
		Dialect:    t.dialect,
		Callsite:   t.callsite,
		Statement:  first(t.stmts),
		Statements: append([]string(nil), t.stmts...),
		Stack:      append([]string(nil), t.stack...),
		DurationMS: time.Since(t.started).Milliseconds(),
		At:         t.started,
		Error:      err,
	}
}

func (t *watchedTx) ageEvent(kind string) TxEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	return TxEvent{
		ID:         t.id,
		Kind:       kind,
		Dialect:    t.dialect,
		Callsite:   t.callsite,
		Statement:  first(t.stmts),
		Statements: append([]string(nil), t.stmts...),
		Stack:      append([]string(nil), t.stack...),
		DurationMS: time.Since(t.started).Milliseconds(),
		AgeMS:      time.Since(t.started).Milliseconds(),
		At:         t.started,
		Error:      t.suspect,
	}
}

type txWatcher struct {
	enabled   atomic.Bool
	threshold atomic.Int64 // nanoseconds
	interval  atomic.Int64 // nanoseconds
	history   atomic.Int64
	seq       atomic.Int64

	mu     sync.Mutex
	open   map[*watchedTx]struct{}
	events []TxEvent

	begun        atomic.Int64
	committed    atomic.Int64
	commitFailed atomic.Int64
	rolledBack   atomic.Int64
	rollbackFail atomic.Int64
	slowTx       atomic.Int64
	slowStmt     atomic.Int64
	poolReturn   atomic.Int64
	longTx       atomic.Int64

	startOnce sync.Once
}

var txw = &txWatcher{open: make(map[*watchedTx]struct{})}

func (w *txWatcher) configure(cfg Config) {
	w.enabled.Store(cfg.TxWatchEnabled)

	threshold := cfg.TxWatchThreshold
	if threshold <= 0 {
		threshold = defaultTxWatchThreshold
	}
	w.threshold.Store(int64(threshold))

	interval := cfg.TxWatchInterval
	if interval <= 0 {
		interval = defaultTxWatchInterval
	}
	w.interval.Store(int64(interval))

	history := cfg.TxWatchHistory
	if history <= 0 {
		history = defaultTxWatchHistory
	}
	w.history.Store(int64(history))

	if cfg.TxWatchEnabled {
		w.startOnce.Do(w.runWatchdog)
	}
}

func (w *txWatcher) record(ev TxEvent) {
	ev.ID = w.seq.Add(1)
	w.mu.Lock()
	defer w.mu.Unlock()
	max := int(w.history.Load())
	if max <= 0 {
		max = defaultTxWatchHistory
	}
	w.events = append(w.events, ev)
	if len(w.events) > max {
		w.events = w.events[len(w.events)-max:]
	}
}

func (w *txWatcher) begin(dialect string, stack []string) *watchedTx {
	tx := &watchedTx{
		id:       w.seq.Add(1),
		dialect:  dialect,
		callsite: first(stack),
		stack:    stack,
		started:  time.Now(),
	}
	w.begun.Add(1)

	w.mu.Lock()
	w.open[tx] = struct{}{}
	w.mu.Unlock()

	return tx
}

func (w *txWatcher) finish(tx *watchedTx) {
	if tx == nil {
		return
	}
	tx.mu.Lock()
	tx.closed = true
	tx.mu.Unlock()

	w.mu.Lock()
	delete(w.open, tx)
	w.mu.Unlock()
}

func (w *txWatcher) complete(tx *watchedTx) {
	if tx == nil {
		return
	}
	duration := time.Since(tx.started)
	if duration >= time.Duration(w.threshold.Load()) {
		w.slowTx.Add(1)
		ev := tx.event("slow_transaction", "")
		ev.DurationMS = duration.Milliseconds()
		log.Warn(context.Background(), "database write transaction was slow",
			log.String("dialect", ev.Dialect),
			log.Duration("duration", duration),
			log.String("callsite", ev.Callsite),
			log.Any("stack", ev.Stack),
			log.String("statement", ev.Statement),
			log.Any("statements", ev.Statements),
		)
		w.record(ev)
	}
	w.finish(tx)
}

// notePoolReturn reports a connection that is being recycled while a transaction
// on it was never finished - the leaked-connection shape. It is only reported
// once per transaction.
func (w *txWatcher) notePoolReturn(tx *watchedTx, source string) {
	if tx == nil {
		return
	}
	tx.mu.Lock()
	if tx.closed {
		tx.mu.Unlock()
		return
	}
	tx.closed = true
	suspect := tx.suspect
	tx.mu.Unlock()

	w.mu.Lock()
	delete(w.open, tx)
	w.mu.Unlock()

	w.poolReturn.Add(1)
	ev := tx.ageEvent("pool_return_with_open_transaction")
	ev.Error = suspect
	log.Warn(context.Background(), "database connection was recycled with an open transaction",
		log.String("source", source),
		log.String("dialect", ev.Dialect),
		log.Duration("age", time.Duration(ev.AgeMS)*time.Millisecond),
		log.String("callsite", ev.Callsite),
		log.Any("stack", ev.Stack),
		log.String("statement", ev.Statement),
		log.Any("statements", ev.Statements),
		log.String("suspect", suspect),
	)
	w.record(ev)
}

// runWatchdog periodically reports transactions that are still open. The
// original incident had no goroutine holding the lock, so this is the only place
// that can say who started it, when, and with which statement.
func (w *txWatcher) runWatchdog() {
	go func() {
		for {
			interval := time.Duration(w.interval.Load())
			if interval <= 0 {
				interval = defaultTxWatchInterval
			}
			time.Sleep(interval)
			if !w.enabled.Load() {
				continue
			}
			threshold := time.Duration(w.threshold.Load())

			w.mu.Lock()
			stale := make([]*watchedTx, 0)
			for tx := range w.open {
				if time.Since(tx.started) >= threshold {
					stale = append(stale, tx)
				}
			}
			w.mu.Unlock()

			for _, tx := range stale {
				ev := tx.ageEvent("long_running_transaction")
				w.longTx.Add(1)
				log.Warn(context.Background(), "database transaction is still open",
					log.String("dialect", ev.Dialect),
					log.Duration("age", time.Since(tx.started)),
					log.String("callsite", ev.Callsite),
					log.Any("stack", ev.Stack),
					log.String("statement", ev.Statement),
					log.Any("statements", ev.Statements),
					log.String("suspect", ev.Error),
				)
				w.record(ev)
			}
		}
	}()
}

// InstrumentationSnapshot returns the current counters and recent events.
func InstrumentationSnapshot() InstrumentationReport {
	snap := InstrumentationReport{
		Enabled:         txw.enabled.Load(),
		ThresholdMS:     time.Duration(txw.threshold.Load()).Milliseconds(),
		WatchIntervalMS: time.Duration(txw.interval.Load()).Milliseconds(),
		Counters: InstrumentationCounters{
			TransactionsBegun:      txw.begun.Load(),
			TransactionsCommitted:  txw.committed.Load(),
			CommitsFailed:          txw.commitFailed.Load(),
			Rollbacks:              txw.rolledBack.Load(),
			RollbacksFailed:        txw.rollbackFail.Load(),
			SlowTransactions:       txw.slowTx.Load(),
			SlowStatements:         txw.slowStmt.Load(),
			PoolReturnsWithOpenTx:  txw.poolReturn.Load(),
			LongTransactionReports: txw.longTx.Load(),
		},
	}

	txw.mu.Lock()
	defer txw.mu.Unlock()
	for tx := range txw.open {
		snap.OpenTransactions = append(snap.OpenTransactions, tx.ageEvent("open"))
	}
	snap.RecentEvents = append(snap.RecentEvents, txw.events...)

	return snap
}

// registerInstrumentedDrivers registers one wrapper per dialect. driverName
// returns these names, so every connection in the process goes through the
// instrumentation regardless of dialect.
func registerInstrumentedDrivers() {
	registerOnce.Do(func() {
		sql.Register(instrumentedSQLiteDriverName, &instrumentedDriver{inner: sqliteshim.NewDriver(), dialect: "sqlite3"})
		sql.Register(instrumentedPGXDriverName, &instrumentedDriver{inner: stdlib.GetDefaultDriver(), dialect: "postgres"})
		sql.Register(instrumentedMySQLDriverName, &instrumentedDriver{inner: &mysql.MySQLDriver{}, dialect: "mysql"})
	})
}

var registerOnce sync.Once

type instrumentedDriver struct {
	inner   driver.Driver
	dialect string
}

func (d *instrumentedDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &instrumentedConn{Conn: conn, dialect: d.dialect}, nil
}

func (d *instrumentedDriver) OpenConnector(name string) (driver.Connector, error) {
	if dc, ok := d.inner.(driver.DriverContext); ok {
		connector, err := dc.OpenConnector(name)
		if err != nil {
			return nil, err
		}
		return &instrumentedConnector{inner: connector, dialect: d.dialect}, nil
	}
	return &instrumentedConnector{inner: &dsnConnector{name: name, drv: d.inner}, dialect: d.dialect}, nil
}

type instrumentedConnector struct {
	inner   driver.Connector
	dialect string
}

func (c *instrumentedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &instrumentedConn{Conn: conn, dialect: c.dialect}, nil
}

func (c *instrumentedConnector) Driver() driver.Driver {
	return &instrumentedDriver{inner: c.inner.Driver(), dialect: c.dialect}
}

// dsnConnector adapts a plain driver.Driver (one without DriverContext) to the
// Connector interface.
type dsnConnector struct {
	name string
	drv  driver.Driver
}

func (c *dsnConnector) Connect(context.Context) (driver.Conn, error) { return c.drv.Open(c.name) }
func (c *dsnConnector) Driver() driver.Driver                        { return c.drv }

type instrumentedConn struct {
	driver.Conn

	dialect string

	mu       sync.Mutex
	openTx   *watchedTx
	lastStmt string
}

func (c *instrumentedConn) noteStatement(query string) {
	c.mu.Lock()
	last := c.lastStmt
	c.lastStmt = truncate(query, txWatchMaxStatementLen)
	tx := c.openTx
	c.mu.Unlock()

	if tx != nil {
		tx.note(query)
	}
	_ = last
}

func (c *instrumentedConn) trackTx(tx *watchedTx) {
	c.mu.Lock()
	c.openTx = tx
	c.mu.Unlock()
}

func (c *instrumentedConn) clearTx(tx *watchedTx) {
	c.mu.Lock()
	if c.openTx == tx {
		c.openTx = nil
	}
	c.mu.Unlock()
}

func (c *instrumentedConn) currentTx() *watchedTx {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.openTx
}

func (c *instrumentedConn) Prepare(query string) (driver.Stmt, error) {
	c.noteStatement(query)
	return c.Conn.Prepare(query)
}

func (c *instrumentedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.noteStatement(query)
	if pc, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}
	return c.Conn.Prepare(query)
}

func (c *instrumentedConn) Close() error {
	if tx := c.currentTx(); tx != nil {
		txw.notePoolReturn(tx, "close")
	}
	return c.Conn.Close()
}

func (c *instrumentedConn) ResetSession(ctx context.Context) error {
	if tx := c.currentTx(); tx != nil {
		txw.notePoolReturn(tx, "reset_session")
	}
	if rs, ok := c.Conn.(driver.SessionResetter); ok {
		return rs.ResetSession(ctx)
	}
	return nil
}

func (c *instrumentedConn) IsValid() bool {
	if v, ok := c.Conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

func (c *instrumentedConn) CheckNamedValue(nv *driver.NamedValue) error {
	if ck, ok := c.Conn.(driver.NamedValueChecker); ok {
		return ck.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

func (c *instrumentedConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *instrumentedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var (
		inner driver.Tx
		err   error
	)
	if bt, ok := c.Conn.(driver.ConnBeginTx); ok {
		inner, err = bt.BeginTx(ctx, opts)
	} else {
		if opts.Isolation != 0 || opts.ReadOnly {
			return nil, driver.ErrSkip
		}
		inner, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, err
	}

	tx := txw.begin(c.dialect, callsites())
	c.trackTx(tx)

	return &instrumentedTx{Tx: inner, conn: c, watched: tx}, nil
}

func (c *instrumentedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.noteStatement(query)
	start := time.Now()

	var (
		res driver.Result
		err error
	)
	if ec, ok := c.Conn.(driver.ExecerContext); ok {
		res, err = ec.ExecContext(ctx, query, args)
	} else {
		res, err = execWithoutContext(ctx, c.Conn, query, args)
	}

	c.noteSlowStatement(query, time.Since(start), err)

	return res, err
}

func (c *instrumentedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.noteStatement(query)
	start := time.Now()

	var (
		rows driver.Rows
		err  error
	)
	if qc, ok := c.Conn.(driver.QueryerContext); ok {
		rows, err = qc.QueryContext(ctx, query, args)
	} else {
		rows, err = queryWithoutContext(ctx, c.Conn, query, args)
	}

	c.noteSlowStatement(query, time.Since(start), err)

	return rows, err
}

func (c *instrumentedConn) Ping(ctx context.Context) error {
	if p, ok := c.Conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (c *instrumentedConn) noteSlowStatement(query string, elapsed time.Duration, err error) {
	if elapsed < time.Duration(txw.threshold.Load()) {
		return
	}
	txw.slowStmt.Add(1)
	stack := callsites()
	ev := TxEvent{
		Kind:       "slow_statement",
		Dialect:    c.dialect,
		Callsite:   first(stack),
		Stack:      stack,
		Statement:  truncate(query, txWatchMaxStatementLen),
		DurationMS: elapsed.Milliseconds(),
		At:         time.Now().Add(-elapsed),
	}
	if err != nil {
		ev.Error = err.Error()
	}
	log.Warn(context.Background(), "database statement was slow",
		log.String("dialect", ev.Dialect),
		log.Duration("duration", elapsed),
		log.String("callsite", ev.Callsite),
		log.Any("stack", ev.Stack),
		log.String("statement", ev.Statement),
		log.Cause(err),
	)
	txw.record(ev)
}

type instrumentedTx struct {
	driver.Tx

	conn    *instrumentedConn
	watched *watchedTx
}

func (t *instrumentedTx) Commit() error {
	err := t.Tx.Commit()
	if err != nil {
		txw.commitFailed.Add(1)
		t.watched.mu.Lock()
		if t.watched.suspect == "" {
			t.watched.suspect = "commit failed: " + err.Error()
		}
		t.watched.mu.Unlock()

		ev := t.watched.event("commit_failed", err.Error())
		log.Error(context.Background(), "database transaction commit failed",
			log.String("dialect", ev.Dialect),
			log.Duration("duration", time.Since(t.watched.started)),
			log.String("callsite", ev.Callsite),
			log.Any("stack", ev.Stack),
			log.String("statement", ev.Statement),
			log.Any("statements", ev.Statements),
			log.Cause(err),
		)
		txw.record(ev)

		// database/sql treats the transaction as finished once Commit returns,
		// even on error, so the connection may be reused with the driver still
		// holding it. Keep the record open: the pool-return check or the
		// watchdog will report it instead of the lock quietly pinning.
		return err
	}

	txw.committed.Add(1)
	txw.complete(t.watched)
	t.conn.clearTx(t.watched)

	return nil
}

func (t *instrumentedTx) Rollback() error {
	err := t.Tx.Rollback()
	if err != nil {
		txw.rollbackFail.Add(1)
		ev := t.watched.event("rollback_failed", err.Error())
		log.Error(context.Background(), "database transaction rollback failed",
			log.String("dialect", ev.Dialect),
			log.String("callsite", ev.Callsite),
			log.Any("stack", ev.Stack),
			log.String("statement", ev.Statement),
			log.Cause(err),
		)
		txw.record(ev)
	} else {
		txw.rolledBack.Add(1)
	}

	txw.finish(t.watched)
	t.conn.clearTx(t.watched)

	return err
}

func execWithoutContext(ctx context.Context, conn driver.Conn, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := prepare(ctx, conn, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if ec, ok := stmt.(driver.StmtExecContext); ok {
		return ec.ExecContext(ctx, args)
	}
	return stmt.Exec(namedValues(args))
}

func queryWithoutContext(ctx context.Context, conn driver.Conn, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := prepare(ctx, conn, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if qc, ok := stmt.(driver.StmtQueryContext); ok {
		return qc.QueryContext(ctx, args)
	}
	return stmt.Query(namedValues(args))
}

func prepare(ctx context.Context, conn driver.Conn, query string) (driver.Stmt, error) {
	if pc, ok := conn.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}
	return conn.Prepare(query)
}

func namedValues(args []driver.NamedValue) []driver.Value {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return values
}

// callsite walks the stack past database/sql and past the wrapper frames of
// this file to name the code that opened the transaction. Only the wrapper
// itself is skipped: a helper elsewhere in this package is a legitimate caller.
// callsites returns the call chain that led here, innermost first, skipping the
// wrapper frames of this file(). The immediate caller is normally the ORM or a
// driver helper, which names the operation but not the code path that holds the
// lock; frames from this repository are therefore collected first and
// dependency frames are only used when nothing else is available.
func callsites() []string {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(1, pcs)
	frames := runtime.CallersFrames(pcs[:n])

	var (
		repo []string
		dep  []string
	)
	for {
		frame, more := frames.Next()
		if !isWrapperFrame(frame.Function) {
			loc := truncate(fmt.Sprintf("%s:%d %s", frame.File, frame.Line, shortFunc(frame.Function)), txWatchMaxCallsiteLen)
			if isDependencyFrame(frame.File) {
				if len(dep) < txWatchMaxStack {
					dep = append(dep, loc)
				}
			} else if len(repo) < txWatchMaxStack {
				repo = append(repo, loc)
			}
		}
		if !more {
			break
		}
	}

	if len(repo) > 0 {
		return repo
	}
	return dep
}

// isDependencyFrame reports whether a frame comes from the module cache or a
// vendor directory rather than from this repository.
func isDependencyFrame(file string) bool {
	return strings.Contains(file, "/pkg/mod/") || strings.Contains(file, "/vendor/")
}

func isWrapperFrame(fn string) bool {
	if strings.HasPrefix(fn, "database/sql.") || strings.HasPrefix(fn, "reflect.") {
		return true
	}
	const pkg = "github.com/looplj/axonhub/internal/server/db."
	if !strings.HasPrefix(fn, pkg) {
		return false
	}
	rest := strings.TrimPrefix(fn, pkg)
	for _, prefix := range []string{
		"callsite", "callsites", "isWrapperFrame", "prepare", "execWithoutContext", "queryWithoutContext",
		"registerInstrumentedDrivers", "(*instrumentedConn)", "(*instrumentedTx)",
		"(*instrumentedDriver)", "(*instrumentedConnector)",
	} {
		if strings.HasPrefix(rest, prefix) {
			return true
		}
	}
	return false
}

func shortFunc(fn string) string {
	if idx := strings.LastIndex(fn, "/"); idx >= 0 {
		return fn[idx+1:]
	}
	return fn
}

func truncate(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
