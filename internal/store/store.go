// Package store is the SQLite persistence layer: orders and leads pushed to
// the CRM, tracked parcels, processed calls and the outbox of CRM writes.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("not found")

const schema = `
CREATE TABLE IF NOT EXISTS orders (
  external_id TEXT PRIMARY KEY, source TEXT NOT NULL, phone TEXT NOT NULL DEFAULT '',
  contact TEXT NOT NULL DEFAULT '', sd_order_id INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS orders_phone ON orders(source, phone, created_at);
CREATE TABLE IF NOT EXISTS outbox (
  id INTEGER PRIMARY KEY AUTOINCREMENT, dedupe_key TEXT NOT NULL UNIQUE, kind TEXT NOT NULL,
  payload TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  next_at INTEGER NOT NULL, last_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox(state, next_at);
CREATE TABLE IF NOT EXISTS ttns (
  number TEXT PRIMARY KEY, external_id TEXT NOT NULL DEFAULT '', sd_order_id INTEGER NOT NULL DEFAULT 0,
  last_code INTEGER NOT NULL DEFAULT 0, last_rank INTEGER NOT NULL DEFAULT 0, final INTEGER NOT NULL DEFAULT 0,
  added_at INTEGER NOT NULL, polled_at INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS calls (
  general_call_id TEXT PRIMARY KEY, phone TEXT NOT NULL, kind TEXT NOT NULL, disposition TEXT NOT NULL,
  lead_external_id TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL);
`

const (
	JobPending = "pending"
	JobDone    = "done"
	JobFailed  = "failed"

	KindCreateOrder = "create_order"
	KindNote        = "note"
	KindStatus      = "status"

	SourceCall = "call"
)

type Order struct {
	ExternalID string    `json:"external_id"`
	Source     string    `json:"source"`
	Phone      string    `json:"phone"`
	Contact    string    `json:"contact,omitempty"`
	SDOrderID  int64     `json:"salesdrive_order_id"`
	CreatedAt  time.Time `json:"created_at"`
}

type Job struct {
	ID        int64     `json:"id"`
	DedupeKey string    `json:"key"`
	Kind      string    `json:"kind"`
	Payload   []byte    `json:"-"`
	State     string    `json:"state"`
	Attempts  int       `json:"attempts"`
	NextAt    time.Time `json:"next_at"`
	LastError string    `json:"last_error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type TTN struct {
	Number     string    `json:"number"`
	ExternalID string    `json:"external_id,omitempty"`
	SDOrderID  int64     `json:"salesdrive_order_id,omitempty"`
	LastCode   int       `json:"last_code"`
	LastRank   int       `json:"-"`
	Final      bool      `json:"final"`
	AddedAt    time.Time `json:"added_at"`
	PolledAt   time.Time `json:"polled_at"`
}

var memSeq atomic.Int64

type Store struct{ db *sql.DB }

func Open(path string) (*Store, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	if path == ":memory:" {
		dsn = fmt.Sprintf("file:mem%d?mode=memory&cache=shared&_pragma=busy_timeout(5000)", memSeq.Add(1))
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // one writer; makes every Tx serial and deadlock-free
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

type Tx struct{ tx *sql.Tx }

func (s *Store) Do(ctx context.Context, fn func(*Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(&Tx{tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func isUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
func ts(t time.Time) int64 { return t.Unix() }

// InsertOrder returns false when the external id is already known.
func (t *Tx) InsertOrder(ctx context.Context, o Order) (bool, error) {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO orders(external_id,source,phone,contact,created_at) VALUES(?,?,?,?,?)`,
		o.ExternalID, o.Source, o.Phone, o.Contact, ts(o.CreatedAt))
	if isUnique(err) {
		return false, nil
	}
	return err == nil, err
}

func (t *Tx) GetOrder(ctx context.Context, ext string) (*Order, error) {
	var o Order
	var c int64
	err := t.tx.QueryRowContext(ctx, `SELECT external_id,source,phone,contact,sd_order_id,created_at FROM orders WHERE external_id=?`, ext).
		Scan(&o.ExternalID, &o.Source, &o.Phone, &o.Contact, &o.SDOrderID, &c)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	o.CreatedAt = time.Unix(c, 0)
	return &o, err
}

// SetSDOrderID records the CRM id; it never overwrites one that is already set.
func (t *Tx) SetSDOrderID(ctx context.Context, ext string, id int64) error {
	_, err := t.tx.ExecContext(ctx, `UPDATE orders SET sd_order_id=? WHERE external_id=? AND sd_order_id=0`, id, ext)
	return err
}

// RecentLead finds the newest lead created from a missed call by this phone
// since the given time.
func (t *Tx) RecentLead(ctx context.Context, phone string, since time.Time) (*Order, error) {
	var ext string
	err := t.tx.QueryRowContext(ctx, `SELECT external_id FROM orders WHERE source=? AND phone=? AND created_at>=?
		ORDER BY created_at DESC, rowid DESC LIMIT 1`, SourceCall, phone, ts(since)).Scan(&ext)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return t.GetOrder(ctx, ext)
}

// InsertCall returns false for a call event that was already processed.
func (t *Tx) InsertCall(ctx context.Context, id, phone, kind, disposition string, at time.Time) (bool, error) {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO calls(general_call_id,phone,kind,disposition,created_at) VALUES(?,?,?,?,?)`,
		id, phone, kind, disposition, ts(at))
	if isUnique(err) {
		return false, nil
	}
	return err == nil, err
}

// Enqueue adds an outbox job; false when the dedupe key already exists.
func (t *Tx) Enqueue(ctx context.Context, key, kind string, payload []byte, at time.Time) (bool, error) {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO outbox(dedupe_key,kind,payload,state,next_at,created_at) VALUES(?,?,?,?,?,?)`,
		key, kind, string(payload), JobPending, ts(at), ts(at))
	if isUnique(err) {
		return false, nil
	}
	return err == nil, err
}

const jobCols = `id,dedupe_key,kind,payload,state,attempts,next_at,last_error,created_at`

func scanJob(sc interface{ Scan(...any) error }) (Job, error) {
	var j Job
	var p string
	var n, c int64
	err := sc.Scan(&j.ID, &j.DedupeKey, &j.Kind, &p, &j.State, &j.Attempts, &n, &j.LastError, &c)
	j.Payload, j.NextAt, j.CreatedAt = []byte(p), time.Unix(n, 0), time.Unix(c, 0)
	return j, err
}

func (t *Tx) jobs(ctx context.Context, q string, args ...any) ([]Job, error) {
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (t *Tx) DueJobs(ctx context.Context, now time.Time, limit int) ([]Job, error) {
	return t.jobs(ctx, `SELECT `+jobCols+` FROM outbox WHERE state='pending' AND next_at<=? ORDER BY id LIMIT ?`, ts(now), limit)
}

func (t *Tx) JobsByState(ctx context.Context, state string, limit int) ([]Job, error) {
	if state == "" {
		return t.jobs(ctx, `SELECT `+jobCols+` FROM outbox ORDER BY id DESC LIMIT ?`, limit)
	}
	return t.jobs(ctx, `SELECT `+jobCols+` FROM outbox WHERE state=? ORDER BY id DESC LIMIT ?`, state, limit)
}

func (t *Tx) JobByKey(ctx context.Context, key string) (*Job, error) {
	j, err := scanJob(t.tx.QueryRowContext(ctx, `SELECT `+jobCols+` FROM outbox WHERE dedupe_key=?`, key))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &j, err
}

func (t *Tx) UpdateJob(ctx context.Context, j Job) error {
	_, err := t.tx.ExecContext(ctx, `UPDATE outbox SET state=?,attempts=?,next_at=?,last_error=? WHERE id=?`,
		j.State, j.Attempts, ts(j.NextAt), j.LastError, j.ID)
	return err
}

func (t *Tx) Requeue(ctx context.Context, id int64, now time.Time) (bool, error) {
	res, err := t.tx.ExecContext(ctx, `UPDATE outbox SET state='pending',attempts=0,next_at=?,last_error='' WHERE id=? AND state='failed'`, ts(now), id)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// AddTTN starts tracking a parcel; an existing one only gains missing links.
func (t *Tx) AddTTN(ctx context.Context, number, ext string, sdID int64, at time.Time) (bool, error) {
	res, err := t.tx.ExecContext(ctx, `INSERT OR IGNORE INTO ttns(number,external_id,sd_order_id,added_at) VALUES(?,?,?,?)`, number, ext, sdID, ts(at))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		_, err = t.tx.ExecContext(ctx, `UPDATE ttns SET external_id=CASE WHEN external_id='' THEN ? ELSE external_id END,
			sd_order_id=CASE WHEN sd_order_id=0 THEN ? ELSE sd_order_id END WHERE number=?`, ext, sdID, number)
	}
	return n == 1, err
}

func scanTTN(sc interface{ Scan(...any) error }) (TTN, error) {
	var x TTN
	var fin int
	var a, p int64
	err := sc.Scan(&x.Number, &x.ExternalID, &x.SDOrderID, &x.LastCode, &x.LastRank, &fin, &a, &p)
	x.Final, x.AddedAt, x.PolledAt = fin == 1, time.Unix(a, 0), time.Unix(p, 0)
	return x, err
}

const ttnCols = `number,external_id,sd_order_id,last_code,last_rank,final,added_at,polled_at`

// DueTTNs are unfinished parcels not polled since the given time.
func (t *Tx) DueTTNs(ctx context.Context, polledBefore time.Time, limit int) ([]TTN, error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT `+ttnCols+` FROM ttns WHERE final=0 AND polled_at<=? ORDER BY polled_at, number LIMIT ?`, ts(polledBefore), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TTN
	for rows.Next() {
		x, err := scanTTN(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (t *Tx) GetTTN(ctx context.Context, number string) (*TTN, error) {
	x, err := scanTTN(t.tx.QueryRowContext(ctx, `SELECT `+ttnCols+` FROM ttns WHERE number=?`, number))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &x, err
}

func (t *Tx) UpdateTTN(ctx context.Context, x TTN) error {
	fin := 0
	if x.Final {
		fin = 1
	}
	_, err := t.tx.ExecContext(ctx, `UPDATE ttns SET last_code=?,last_rank=?,final=?,polled_at=? WHERE number=?`,
		x.LastCode, x.LastRank, fin, ts(x.PolledAt), x.Number)
	return err
}

func (t *Tx) TTNsForOrder(ctx context.Context, ext string) ([]TTN, error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT `+ttnCols+` FROM ttns WHERE external_id=? ORDER BY number`, ext)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TTN
	for rows.Next() {
		x, err := scanTTN(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
