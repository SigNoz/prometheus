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
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2" // register SQL driver

	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/sirupsen/logrus"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/base"
)

const (
	subsystem = "clickhouse"
)

// clickHouse implements storage interface for the ClickHouse.
type clickHouse struct {
	db                   *sql.DB
	l                    *logrus.Entry
	database             string
	maxTimeSeriesInQuery int

	timeSeriesRW sync.RWMutex
	// map of [metirc_name][fingerprint][labels]
	timeSeries          map[string]map[uint64][]*prompb.Label
	lastLoadedTimeStamp int64
	useClickHouseQuery  bool
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

	options := &clickhouse.Options{
		Addr: []string{dsnURL.Host},
	}
	if dsnURL.Query().Get("username") != "" {
		auth := clickhouse.Auth{
			// Database: "",
			Username: dsnURL.Query().Get("username"),
			Password: dsnURL.Query().Get("password"),
		}

		options.Auth = auth
	}
	initDB := clickhouse.OpenDB(options)

	initDB.SetConnMaxIdleTime(2)
	initDB.SetMaxOpenConns(params.MaxOpenConns)
	initDB.SetConnMaxLifetime(0)

	if err != nil {
		return nil, fmt.Errorf("could not connect to clickhouse: %s", err)
	}

	var useClickHouseQuery bool
	if strings.ToLower(strings.TrimSpace(os.Getenv("USE_CLICKHOUSE_QUERY"))) == "true" {
		useClickHouseQuery = true
	}

	ch := &clickHouse{
		db:                   initDB,
		l:                    l,
		database:             database,
		maxTimeSeriesInQuery: params.MaxTimeSeriesInQuery,

		timeSeries:         make(map[string]map[uint64][]*prompb.Label, 262144),
		useClickHouseQuery: useClickHouseQuery,
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
	v, err := mem.VirtualMemory()

	if err != nil {
		ch.l.Errorf("could not get memory info: %s", err)
		return
	}

	// in batches with maximum 1/5 of memory, assuming 1.5kb avg per time series
	batchSize := v.Available / 5 / 1536

	// LIMIT n, m allows to select the m rows from the result after skipping the first n rows.
	// The LIMIT m OFFSET n syntax is equivalent.
	queryTmpl := `SELECT DISTINCT fingerprint, labels FROM %s.time_series_v2 WHERE timestamp_ms >= $1 LIMIT $2, $3;`

	for {
		var totalSeries uint64
		var offset uint64
		countQuery := fmt.Sprintf(`SELECT COUNT(DISTINCT(fingerprint)) FROM %s.time_series_v2 WHERE timestamp_ms >= $1`, ch.database)
		err = ch.db.QueryRow(countQuery, ch.lastLoadedTimeStamp).Scan(&totalSeries)
		if err != nil {
			ch.l.Errorf("could not get the total series count: %s", err)
			return
		}

		lastLoadedTimeStamp := time.Now().Add(time.Duration(-1) * time.Minute).UnixMilli()

		for ; offset <= totalSeries; offset += batchSize + 1 {
			timeSeries := make(map[string]map[uint64][]*prompb.Label)
			newSeriesCount := 0

			err := func() error {
				query := fmt.Sprintf(queryTmpl, ch.database)
				ch.l.Debug("Running reloader query:", query, ch.lastLoadedTimeStamp, offset, batchSize)
				rows, err := ch.db.Query(query, ch.lastLoadedTimeStamp, offset, batchSize)
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
					labels, metricName, err := unmarshalLabels(b)
					if err != nil {
						return err
					}
					if _, ok := timeSeries[metricName]; !ok {
						timeSeries[metricName] = make(map[uint64][]*prompb.Label)
					}
					if _, ok := timeSeries[metricName][f]; !ok {
						timeSeries[metricName][f] = make([]*prompb.Label, 0)
					}
					timeSeries[metricName][f] = labels
					newSeriesCount += 1
				}
				return rows.Err()
			}()
			if err == nil {
				ch.timeSeriesRW.Lock()
				for metricName, fingerprintsMap := range timeSeries {
					for fingerprint, labels := range fingerprintsMap {
						if _, ok := ch.timeSeries[metricName]; !ok {
							ch.timeSeries[metricName] = make(map[uint64][]*prompb.Label)
						}
						ch.timeSeries[metricName][fingerprint] = labels
					}
				}
				ch.timeSeriesRW.Unlock()
				ch.l.Infof("Loaded %d new time series", newSeriesCount)
			} else {
				ch.l.Error(err)
			}
		}
		ch.timeSeriesRW.Lock()
		ch.lastLoadedTimeStamp = lastLoadedTimeStamp
		ch.timeSeriesRW.Unlock()
		select {
		case <-ctx.Done():
			ch.l.Warn(ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

func (ch *clickHouse) scanSamples(rows *sql.Rows) ([]*prompb.TimeSeries, error) {
	// scan results
	var res []*prompb.TimeSeries
	var ts *prompb.TimeSeries
	var fingerprint, prevFingerprint uint64
	var timestampMs int64
	var value float64
	var metricName string
	for rows.Next() {
		if err := rows.Scan(&metricName, &fingerprint, &timestampMs, &value); err != nil {
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
			labels := ch.timeSeries[metricName][fingerprint]
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

func (ch *clickHouse) querySamples(ctx context.Context, start, end int64, fingerprints map[uint64]struct{}, metricName string) ([]*prompb.TimeSeries, error) {

	var fingerprintsKeys string
	for f := range fingerprints {
		fingerprintsKeys += fmt.Sprintf("'%v',", f)
	}
	fingerprintsKeys = fingerprintsKeys[:len(fingerprintsKeys)-1] // cut last ", "
	query := fmt.Sprintf(`
		SELECT metric_name, fingerprint, timestamp_ms, value
			FROM %s.samples_v2
			WHERE metric_name = '%s' AND fingerprint IN (%s) AND timestamp_ms >= %d AND timestamp_ms <= %d ORDER BY fingerprint, timestamp_ms;`,
		ch.database, metricName, fingerprintsKeys, start, end,
	)
	query = strings.TrimSpace(query)

	ch.l.Debugf("Running query : %s", query)

	// run query
	rows, err := ch.db.QueryContext(ctx, query)
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

func (ch *clickHouse) prepareClickHouseQuery(query *prompb.Query, metricName string) (string, []interface{}, error) {
	var clickHouseQuery string
	var conditions []string
	var args []interface{}
	var argCount int
	conditions = append(conditions, fmt.Sprintf("metric_name = $%d", argCount+1))
	args = append(args, metricName)
	for _, m := range query.Matchers {
		switch m.Type {
		case prompb.LabelMatcher_EQ:
			conditions = append(conditions, fmt.Sprintf("JSONExtractString(labels, $%d) = $%d", argCount+2, argCount+3))
		case prompb.LabelMatcher_NEQ:
			conditions = append(conditions, fmt.Sprintf("JSONExtractString(labels, $%d) != $%d", argCount+2, argCount+3))
		case prompb.LabelMatcher_RE:
			conditions = append(conditions, fmt.Sprintf("match(JSONExtractString(labels, $%d), $%d)", argCount+2, argCount+3))
		case prompb.LabelMatcher_NRE:
			conditions = append(conditions, fmt.Sprintf("not match(JSONExtractString(labels, $%d), $%d)", argCount+2, argCount+3))
		default:
			return "", nil, fmt.Errorf("prepareClickHouseQuery: unexpected matcher %d", m.Type)
		}
		args = append(args, m.Name, m.Value)
		argCount += 2
	}
	whereClause := strings.Join(conditions, " AND ")

	clickHouseQuery = fmt.Sprintf(`SELECT DISTINCT fingerprint FROM %s.time_series_v2 WHERE %s`, ch.database, whereClause)
	return clickHouseQuery, args, nil
}

func (ch *clickHouse) fingerprintsForQuery(ctx context.Context, query string, args []interface{}) (map[uint64]struct{}, error) {
	// run query
	rows, err := ch.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// scan results
	var fingerprint uint64
	fingerprints := make(map[uint64]struct{})
	for rows.Next() {
		if err := rows.Scan(&fingerprint); err != nil {
			return nil, err
		}
		fingerprints[fingerprint] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return fingerprints, nil
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

	var metricName string
	for _, matcher := range convertedQuery.Matchers {
		if matcher.Name == "__name__" {
			metricName = matcher.Value
		}
	}

	var fingerprints map[uint64]struct{}
	var err error

	if ch.useClickHouseQuery {
		clickHouseQuery, args, err := ch.prepareClickHouseQuery(query, metricName)
		if err != nil {
			return nil, err
		}
		fingerprints, err = ch.fingerprintsForQuery(ctx, clickHouseQuery, args)
		if err != nil {
			return nil, err
		}
	} else {
		fingerprints = make(map[uint64]struct{}, 64)
		// find matching time series
		ch.timeSeriesRW.RLock()
		for f, labels := range ch.timeSeries[metricName] {
			if convertedQuery.Matchers.MatchLabels(labels) {
				fingerprints[f] = struct{}{}
			}
		}
		ch.timeSeriesRW.RUnlock()
	}

	res := new(prompb.QueryResult)
	if len(fingerprints) == 0 {
		return res, nil
	}

	sampleFunc := ch.querySamples
	// if len(fingerprints) > ch.maxTimeSeriesInQuery {
	// 	sampleFunc = ch.tempTableSamples
	// }

	ts, err := sampleFunc(ctx, int64(query.StartTimestampMs), int64(query.EndTimestampMs), fingerprints, metricName)

	if err != nil {
		return nil, err
	}
	res.Timeseries = ts

	return res, nil
}
