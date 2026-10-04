package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeDriver is a driver.Driver that can be told to fail or stall its COMMIT, so
// the instrumentation can be exercised without a real database.
type fakeDriver struct {
	commitErr error
	commitDur time.Duration
}

type fakeConn struct{ d *fakeDriver }

type fakeTx struct {
	d       *fakeDriver
	done    bool
	rollbak int
}

type fakeStmt struct{}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }
func (s *fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}

func (s *fakeStmt) Query([]driver.Value) (driver.Rows, error) { return &fakeRows{}, nil }

type fakeRows struct{ done bool }

func (r *fakeRows) Columns() []string              { return []string{"x"} }
func (r *fakeRows) Close() error                   { return nil }
func (r *fakeRows) Next(dest []driver.Value) error { return errStopRows }

var errStopRows = errors.New("no more rows")

func (d *fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{d: d}, nil }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return &fakeStmt{}, nil }

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) { return &fakeTx{d: c.d}, nil }

// ResetSession makes database/sql call back when this connection is recycled,
// which is where a leaked transaction becomes visible.
func (c *fakeConn) ResetSession(context.Context) error { return nil }

func (t *fakeTx) Commit() error {
	if t.d.commitDur > 0 {
		time.Sleep(t.d.commitDur)
	}
	if t.d.commitErr != nil {
		return t.d.commitErr
	}
	t.done = true
	return nil
}

func (t *fakeTx) Rollback() error {
	t.done = true
	t.rollbak++
	return nil
}

// newTestDB wires the fake driver through the same connector the production
// drivers use, so the test covers the wrapper path rather than a stand-in.
func newTestDB(t *testing.T, drv *fakeDriver, threshold time.Duration) *sql.DB {
	t.Helper()

	reset := func() {
		txw = &txWatcher{open: make(map[*watchedTx]struct{})}
		txw.configure(Config{TxWatchEnabled: true, TxWatchThreshold: threshold, TxWatchInterval: time.Hour, TxWatchHistory: 20})
	}
	reset()
	t.Cleanup(reset)

	return sql.OpenDB(&instrumentedConnector{inner: &dsnConnector{name: "fake", drv: drv}, dialect: "fake"})
}

func eventKinds() []string {
	snap := InstrumentationSnapshot()
	kinds := make([]string, 0, len(snap.RecentEvents))
	for _, ev := range snap.RecentEvents {
		kinds = append(kinds, ev.Kind)
	}
	return kinds
}

func TestInstrumentationRecordsFailedCommitAndPoolReturn(t *testing.T) {
	db := newTestDB(t, &fakeDriver{commitErr: errors.New("database is locked (5) (SQLITE_BUSY)")}, time.Hour)
	defer db.Close()

	db.SetMaxOpenConns(1)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.Error(t, tx.Commit())

	snap := InstrumentationSnapshot()
	require.Equal(t, int64(1), snap.Counters.TransactionsBegun)
	require.Equal(t, int64(1), snap.Counters.CommitsFailed)
	require.Equal(t, int64(0), snap.Counters.TransactionsCommitted)
	require.Contains(t, eventKinds(), "commit_failed")
	// The transaction is still tracked: database/sql dropped its handle even
	// though the driver may still hold the lock.
	require.Len(t, snap.OpenTransactions, 1)

	// Reusing the pooled connection is where the leak shows up: the connection
	// is recycled while the transaction was never finished.
	row := db.QueryRowContext(ctx, "select 1")
	var v int
	if err := row.Scan(&v); err != nil && !errors.Is(err, errStopRows) {
		require.NoError(t, err, "the fake connection should still be usable")
	}

	snap = InstrumentationSnapshot()
	require.Equal(t, int64(1), snap.Counters.PoolReturnsWithOpenTx, "pool return with an open transaction must be reported")
	require.Empty(t, snap.OpenTransactions, "the recycled connection must stop being tracked")
	require.Contains(t, eventKinds(), "pool_return_with_open_transaction")
}

func TestInstrumentationRecordsSlowTransactionAndStatement(t *testing.T) {
	db := newTestDB(t, &fakeDriver{commitDur: 40 * time.Millisecond}, 5*time.Millisecond)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	snap := InstrumentationSnapshot()
	require.Equal(t, int64(1), snap.Counters.TransactionsCommitted)
	require.Equal(t, int64(1), snap.Counters.SlowTransactions)
	require.Contains(t, eventKinds(), "slow_transaction")

	var slow TxEvent
	for _, ev := range snap.RecentEvents {
		if ev.Kind == "slow_transaction" {
			slow = ev
		}
	}
	require.GreaterOrEqual(t, slow.DurationMS, int64(1))
	require.Contains(t, slow.Callsite, "instrumentation_test.go", "the caller that opened the transaction must be named")
}

func TestInstrumentationStaysQuietOnCleanTransactions(t *testing.T) {
	db := newTestDB(t, &fakeDriver{}, time.Hour)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	snap := InstrumentationSnapshot()
	require.Equal(t, int64(2), snap.Counters.TransactionsBegun)
	require.Equal(t, int64(1), snap.Counters.TransactionsCommitted)
	require.Equal(t, int64(1), snap.Counters.Rollbacks)
	require.Equal(t, int64(0), snap.Counters.SlowTransactions)
	require.Equal(t, int64(0), snap.Counters.SlowStatements)
	require.Equal(t, int64(0), snap.Counters.PoolReturnsWithOpenTx)
	require.Empty(t, snap.OpenTransactions)
	require.Empty(t, snap.RecentEvents, "clean work must not fill the event ring")
}

func TestInstrumentationCapturesCallChain(t *testing.T) {
	db := newTestDB(t, &fakeDriver{}, time.Hour)
	defer db.Close()

	tx := openFromHelper(t, db)
	t.Cleanup(func() { _ = tx.Rollback() })

	stack := InstrumentationSnapshot().OpenTransactions[0].Stack
	require.GreaterOrEqual(t, len(stack), 2, "the chain must keep the frames above the immediate caller: %v", stack)
	require.Contains(t, stack[0], "openFromHelper")
	require.Contains(t, stack[1], "TestInstrumentationCapturesCallChain")
}

// openFromHelper stands in for the ORM: it opens the transaction one level below
// the code that actually triggered the write.
func openFromHelper(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	return tx
}

func TestInstrumentationSnapshotExposesOpenTransactionCaller(t *testing.T) {
	db := newTestDB(t, &fakeDriver{}, time.Hour)
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	snap := InstrumentationSnapshot()
	require.Len(t, snap.OpenTransactions, 1)
	open := snap.OpenTransactions[0]
	require.True(t, strings.Contains(open.Callsite, "instrumentation_test.go"), "open transaction must name its caller: %q", open.Callsite)
	require.GreaterOrEqual(t, open.AgeMS, int64(0))

	require.NoError(t, tx.Rollback())
	require.Empty(t, InstrumentationSnapshot().OpenTransactions)
}
