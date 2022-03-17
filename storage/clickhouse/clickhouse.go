// Copyright 2017, 2018 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package clickhouse provides ClickHouse storage.
package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/ClickHouse/clickhouse-go" // register SQL driver
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/base"
)

const (
	namespace = "promhouse"
	subsystem = "clickhouse"
)

// clickHouse implements storage interface for the ClickHouse.
type clickHouse struct {
	db                   *sqlx.DB
	l                    *logrus.Entry
	database             string
	maxTimeSeriesInQuery int

	timeSeriesRW sync.RWMutex
	timeSeries   map[uint64][]*prompb.Label
}

type ClickHouseParams struct {
	DSN                  string
	DropDatabase         bool
	MaxOpenConns         int
	MaxTimeSeriesInQuery int
}

func NewClickHouse(params *ClickHouseParams) (base.Storage, error) {
	l := logrus.WithField("component", "clickhouse")

	dsnURL, err := url.Parse(params.DSN)
	if err != nil {
		return nil, err
	}
	database := dsnURL.Query().Get("database")
	if database == "" {
		return nil, fmt.Errorf("database should be set in ClickHouse DSN")
	}

	var queries []string

	q := dsnURL.Query()

	initURL := dsnURL
	initURL.RawQuery = q.Encode()

	l.Debug("initURL: ", initURL.String())
	initDB, err := sqlx.Open("clickhouse", initURL.String())

	if err != nil {
		l.Debug("Error in connecting using initURL")
		return nil, err
	}
	defer initDB.Close()
	for _, q := range queries {
		q = strings.TrimSpace(q)
		l.Infof("Executing:\n%s", q)
		if _, err = initDB.Exec(q); err != nil {
			return nil, err
		}
	}

	l.Debug("DSN: ", params.DSN)
	// reconnect to created database
	db, err := sqlx.Open("clickhouse", params.DSN)
	if err != nil {
		l.Debug("Error in connecting using DSN")
		return nil, err
	}
	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(params.MaxOpenConns)

	ch := &clickHouse{
		db:                   db,
		l:                    l,
		database:             database,
		maxTimeSeriesInQuery: params.MaxTimeSeriesInQuery,

		timeSeries: make(map[uint64][]*prompb.Label, 8192),
	}

	go func() {
		ctx := pprof.WithLabels(context.TODO(), pprof.Labels("component", "clickhouse_reloader"))
		pprof.SetGoroutineLabels(ctx)
		ch.runTimeSeriesReloader(ctx)
	}()

	return ch, nil
}

func (ch *clickHouse) runTimeSeriesReloader(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	q := fmt.Sprintf(`SELECT DISTINCT fingerprint, labels FROM %s.time_series`, ch.database)
	for {
		ch.timeSeriesRW.RLock()
		timeSeries := make(map[uint64][]*prompb.Label, len(ch.timeSeries))
		ch.timeSeriesRW.RUnlock()

		err := func() error {
			ch.l.Debug(q)
			rows, err := ch.db.Query(q)
			if err != nil {
				return err
			}
			defer rows.Close()

			var f uint64
			var b []byte
			for rows.Next() {
				if err = rows.Scan(&f, &b); err != nil {
					return err
				}
				if timeSeries[f], err = unmarshalLabels(b); err != nil {
					return err
				}
			}
			return rows.Err()
		}()
		if err == nil {
			ch.timeSeriesRW.Lock()
			n := len(timeSeries) - len(ch.timeSeries)
			for f, m := range timeSeries {
				ch.timeSeries[f] = m
			}
			ch.timeSeriesRW.Unlock()
			ch.l.Debugf("Loaded %d existing time series, %d were unknown to this instance.", len(timeSeries), n)
		} else {
			ch.l.Error(err)
		}

		select {
		case <-ctx.Done():
			ch.l.Warn(ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

type beginTxer interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func inTransaction(ctx context.Context, txer beginTxer, f func(*sql.Tx) error) (err error) {
	var tx *sql.Tx
	if tx, err = txer.BeginTx(ctx, nil); err != nil {
		err = errors.WithStack(err)
		return
	}
	defer func() {
		if err == nil {
			err = errors.WithStack(tx.Commit())
		} else {
			tx.Rollback()
		}
	}()
	err = f(tx)
	return
}

func (ch *clickHouse) scanSamples(rows *sql.Rows) ([]*prompb.TimeSeries, error) {
	// scan results
	var res []*prompb.TimeSeries
	var ts *prompb.TimeSeries
	var fingerprint, prevFingerprint uint64
	var timestampMs int64
	var value float64
	for rows.Next() {
		if err := rows.Scan(&fingerprint, &timestampMs, &value); err != nil {
			return nil, errors.WithStack(err)
		}

		// collect samples in time series
		if fingerprint != prevFingerprint {
			// add collected time series to result
			prevFingerprint = fingerprint
			if ts != nil {
				res = append(res, ts)
			}

			// create new time series
			ch.timeSeriesRW.RLock()
			labels := ch.timeSeries[fingerprint]
			ch.timeSeriesRW.RUnlock()
			ts = &prompb.TimeSeries{
				Labels: labels,
			}
		}

		// add samples to current time series
		ts.Samples = append(ts.Samples, prompb.Sample{
			Timestamp: timestampMs,
			Value:     value,
		})
	}

	// add last time series
	if ts != nil {
		res = append(res, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	return res, nil
}

func (ch *clickHouse) querySamples(ctx context.Context, start, end int64, fingerprints map[uint64]struct{}) ([]*prompb.TimeSeries, error) {
	// prepare query string
	placeholders := strings.Repeat("?, ", len(fingerprints))
	query := fmt.Sprintf(`
		SELECT fingerprint, timestamp_ms, value
			FROM %s.samples
			WHERE fingerprint IN (%s) AND timestamp_ms >= ? AND timestamp_ms <= ?
			ORDER BY fingerprint, timestamp_ms`,
		ch.database, placeholders[:len(placeholders)-2], // cut last ", "
	)
	query = strings.TrimSpace(query)
	args := make([]interface{}, 0, len(fingerprints)+2)
	for f := range fingerprints {
		args = append(args, f)
	}
	args = append(args, start)
	args = append(args, end)
	ch.l.Debugf("%s %v", query, args)

	// run query
	rows, err := ch.db.Query(query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	return ch.scanSamples(rows)
}

func (ch *clickHouse) tempTableSamples(ctx context.Context, start, end int64, fingerprints map[uint64]struct{}) ([]*prompb.TimeSeries, error) {
	conn, err := ch.db.Conn(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer conn.Close()

	// create temporary table
	tableName := fmt.Sprintf("promhouse_%d", time.Now().UnixNano())
	query := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s (
			fingerprint UInt64
		)`, tableName)
	query = strings.TrimSpace(query)
	ch.l.Debugf("%s", query)
	if _, err = conn.ExecContext(ctx, query); err != nil {
		return nil, errors.WithStack(err)
	}

	// fill temporary table
	err = inTransaction(ctx, conn, func(tx *sql.Tx) error {
		query = fmt.Sprintf(`INSERT INTO %s (fingerprint) VALUES (?)`, tableName)
		var stmt *sql.Stmt
		var err error
		if stmt, err = tx.PrepareContext(ctx, query); err != nil {
			return errors.WithStack(err)
		}

		for f := range fingerprints {
			// ch.l.Debugf("%s %v", query, f)
			if _, err = stmt.ExecContext(ctx, f); err != nil {
				return errors.WithStack(err)
			}
		}

		return errors.WithStack(stmt.Close())
	})
	if err != nil {
		return nil, err
	}

	// prepare query string
	query = fmt.Sprintf(`
			SELECT fingerprint, timestamp_ms, value
				FROM %s.samples
				INNER JOIN %s USING fingerprint
				WHERE timestamp_ms >= ? AND timestamp_ms <= ?
				ORDER BY fingerprint, timestamp_ms`,
		ch.database, tableName,
	)
	query = strings.TrimSpace(query)
	ch.l.Debugf("%s %v %v", query, start, end)

	// run query
	rows, err := conn.QueryContext(ctx, query, start, end)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	return ch.scanSamples(rows)
}

// convertReadRequest converts protobuf read request into a slice of storage queries.
func (ch *clickHouse) convertReadRequest(request *prompb.Query) base.Query {

	q := base.Query{
		Start:    model.Time(request.StartTimestampMs),
		End:      model.Time(request.EndTimestampMs),
		Matchers: make([]base.Matcher, len(request.Matchers)),
	}

	for j, m := range request.Matchers {
		var t base.MatchType
		switch m.Type {
		case prompb.LabelMatcher_EQ:
			t = base.MatchEqual
		case prompb.LabelMatcher_NEQ:
			t = base.MatchNotEqual
		case prompb.LabelMatcher_RE:
			t = base.MatchRegexp
		case prompb.LabelMatcher_NRE:
			t = base.MatchNotRegexp
		default:
			ch.l.Error("convertReadRequest: unexpected matcher %d", m.Type)
		}

		q.Matchers[j] = base.Matcher{
			Type:  t,
			Name:  m.Name,
			Value: m.Value,
		}
	}

	if request.Hints != nil {
		ch.l.Warnf("Ignoring hint %+v for query %v.", *request.Hints, q)
	}

	return q
}

func (ch *clickHouse) Read(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
	// special case for {{job="rawsql", query="SELECT …"}} (start is ignored)
	if len(query.Matchers) == 2 {
		var hasJob bool
		var queryString string
		for _, m := range query.Matchers {
			if base.MatchType(m.Type) == base.MatchEqual && m.Name == "job" && m.Value == "rawsql" {
				hasJob = true
			}
			if base.MatchType(m.Type) == base.MatchEqual && m.Name == "query" {
				queryString = m.Value
			}
		}
		if hasJob && queryString != "" {
			return ch.readRawSQL(ctx, queryString, int64(query.EndTimestampMs))
		}
	}

	convertedQuery := ch.convertReadRequest(query)

	res := new(prompb.QueryResult)
	// find matching time series
	fingerprints := make(map[uint64]struct{}, 64)
	ch.timeSeriesRW.RLock()
	for f, labels := range ch.timeSeries {
		if convertedQuery.Matchers.MatchLabels(labels) {
			fingerprints[f] = struct{}{}
		}
	}
	ch.timeSeriesRW.RUnlock()
	if len(fingerprints) == 0 {
		return res, nil
	}

	sampleFunc := ch.querySamples
	// if len(fingerprints) > ch.maxTimeSeriesInQuery {
	// 	sampleFunc = ch.tempTableSamples
	// }

	ts, err := sampleFunc(ctx, int64(query.StartTimestampMs), int64(query.EndTimestampMs), fingerprints)

	if err != nil {
		return nil, err
	}
	res.Timeseries = ts

	return res, nil
}
