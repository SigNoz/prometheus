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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2" // register SQL driver

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/base"
)

const (
	subsystem                     = "clickhouse"
	DISTRIBUTED_TIME_SERIES_TABLE = "distributed_time_series_v2"
	DISTRIBUTED_SAMPLES_TABLE     = "distributed_samples_v2"
	DEFAULT_CACHE_SIZE            = 1000000
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
	cache               *Cache
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
	var cacheSize int
	if cacheSizeStr := strings.TrimSpace(os.Getenv("CACHE_SIZE")); cacheSizeStr != "" {
		cacheSize, err = strconv.Atoi(cacheSizeStr)
		if err != nil {
			l.Warnf("Could not parse CACHE_SIZE, using default value (%d)", DEFAULT_CACHE_SIZE)
		}
	}
	if cacheSize == 0 {
		cacheSize = DEFAULT_CACHE_SIZE
	}
	newCache, err := NewCache(cacheSize)
	if err != nil {
		return nil, err
	}

	ch := &clickHouse{
		db:                   initDB,
		l:                    l,
		database:             database,
		maxTimeSeriesInQuery: params.MaxTimeSeriesInQuery,

		timeSeries:         make(map[string]map[uint64][]*prompb.Label, 262144),
		useClickHouseQuery: useClickHouseQuery,
		cache:              newCache,
	}

	go func() {
		ctx := pprof.WithLabels(context.TODO(), pprof.Labels("component", "clickhouse_reloader"))
		pprof.SetGoroutineLabels(ctx)
	}()

	return ch, nil
}

func (ch *clickHouse) scanSamples(rows *sql.Rows) ([]*prompb.TimeSeries, error) {
	// scan results
	var res []*prompb.TimeSeries
	var ts *prompb.TimeSeries
	var fingerprint, prevFingerprint uint64
	var timestampMs int64
	var value float64
	var metricName string

	usedFingerprintCount := 0

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

			labels, ok := ch.cache.Get(fingerprint)
			if !ok {
				return nil, fmt.Errorf("could not find labels for fingerprint %d", fingerprint)
			}
			ts = &prompb.TimeSeries{
				Labels: (labels).([]*prompb.Label),
			}
			usedFingerprintCount += 1
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

func (ch *clickHouse) querySamples(
	ctx context.Context,
	start int64,
	end int64,
	metricName string,
	subQuery string,
	args []interface{},
) ([]*prompb.TimeSeries, error) {

	argCount := len(args)

	query := fmt.Sprintf(`
		SELECT metric_name, fingerprint, timestamp_ms, value
			FROM %s.%s
			WHERE metric_name = $1 AND fingerprint GLOBAL IN (%s) AND timestamp_ms >= $%s AND timestamp_ms <= $%s ORDER BY fingerprint, timestamp_ms;`,
		ch.database, DISTRIBUTED_SAMPLES_TABLE, subQuery, strconv.Itoa(argCount+2), strconv.Itoa(argCount+3))
	query = strings.TrimSpace(query)

	ch.l.Debugf("Running query : %s", query)

	allArgs := append([]interface{}{metricName}, args...)
	allArgs = append(allArgs, start, end)

	// run query
	rows, err := ch.db.QueryContext(ctx, query, allArgs...)
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

func (ch *clickHouse) prepareClickHouseQuery(query *prompb.Query, metricName string, subQuery bool) (string, []interface{}, error) {
	var clickHouseQuery string
	var conditions []string
	var argCount int = 0
	var selectString string = "fingerprint, labels"
	if subQuery {
		argCount = 1
		selectString = "fingerprint"
	}
	var args []interface{}
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

	clickHouseQuery = fmt.Sprintf(`SELECT DISTINCT %s FROM %s.%s WHERE %s`, selectString, ch.database, DISTRIBUTED_TIME_SERIES_TABLE, whereClause)

	return clickHouseQuery, args, nil
}

func (ch *clickHouse) fillMissingFingerprints(ctx context.Context, subQuery string, args []interface{}) {
	// run query
	rows, err := ch.db.QueryContext(ctx, subQuery, args...)
	if err != nil {
		return
	}
	defer rows.Close()

	// scan results
	var fingerprint uint64
	var miss []uint64
	for rows.Next() {
		if err = rows.Scan(&fingerprint); err != nil {
			return
		}
		if _, ok := ch.cache.Get(fingerprint); !ok {
			miss = append(miss, fingerprint)
		}
	}

	if err := rows.Err(); err != nil {
		return
	}

	// fill missing fingerprints in batches of 10k fingerprints with maximum of 10 go routines
	waitCh := make(chan struct{}, 10)
	var wg sync.WaitGroup
	for i := 0; i < len(miss); i += 10000 {
		end := i + 10000
		if end > len(miss) {
			end = len(miss)
		}
		waitCh <- struct{}{}
		wg.Add(1)
		go func(f []uint64) {
			defer wg.Done()
			defer func() { <-waitCh }()
			ch.fillMissingFingerprintsBatch(ctx, f)
		}(miss[i:end])
	}
	wg.Wait()
}

func (ch *clickHouse) fillMissingFingerprintsBatch(ctx context.Context, fingerprints []uint64) error {
	// build query
	var query strings.Builder
	query.WriteString(fmt.Sprintf(`SELECT fingerprint, labels FROM %s.%s WHERE fingerprint IN (`, ch.database, DISTRIBUTED_TIME_SERIES_TABLE))
	for i, fingerprint := range fingerprints {
		if i > 0 {
			query.WriteString(",")
		}
		query.WriteString(fmt.Sprintf("%d", fingerprint))
	}
	query.WriteString(")")

	// run query
	rows, err := ch.db.QueryContext(ctx, query.String())
	if err != nil {
		return err
	}
	defer rows.Close()

	// scan results
	var fingerprint uint64
	var b []byte
	for rows.Next() {
		if err = rows.Scan(&fingerprint, &b); err != nil {
			return err
		}

		labels, _, err := unmarshalLabels(b)

		if err != nil {
			return err
		}
		ch.cache.ContainsOrAdd(fingerprint, labels)
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
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

	var err error

	subQuery, args, err := ch.prepareClickHouseQuery(query, metricName, true)
	if err != nil {
		return nil, err
	}
	start := time.Now()

	ch.fillMissingFingerprints(ctx, subQuery, args)

	ch.l.Infof("fillMissingFingerprints took %s", time.Since(start))

	res := new(prompb.QueryResult)

	ts, err := ch.querySamples(ctx, int64(query.StartTimestampMs), int64(query.EndTimestampMs), metricName, subQuery, args)

	if err != nil {
		return nil, err
	}
	res.Timeseries = ts

	return res, nil
}
