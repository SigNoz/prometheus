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
	"encoding/json"
	"fmt"
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
	subsystem                   = "clickhouse"
	distributedTimeSeriesV4     = "distributed_time_series_v4"
	distributedTimeSeriesV46hrs = "distributed_time_series_v4_6hrs"
	distributedTimeSeriesV41day = "distributed_time_series_v4_1day"
	distributedSamplesV4        = "distributed_samples_v4"
)

// clickHouse implements storage interface for the ClickHouse.
type clickHouse struct {
	db                   *sql.DB
	l                    *logrus.Entry
	database             string
	maxTimeSeriesInQuery int

	timeSeriesRW sync.RWMutex
	// map of [metirc_name][fingerprint][labels]
	timeSeries          map[string]map[uint64][]prompb.Label
	lastLoadedTimeStamp int64
	useClickHouseQuery  bool
}

type ClickHouseParams struct {
	DSN                  string
	DropDatabase         bool
	MaxOpenConns         int
	MaxTimeSeriesInQuery int
}

var (
	sixHoursInMilliseconds = time.Hour.Milliseconds() * 6
	oneDayInMilliseconds   = time.Hour.Milliseconds() * 24
)

// start and end are in milliseconds
func which(start, end int64) (int64, int64, string) {
	// If time range is less than 6 hours, we need to use the `time_series_v4` table
	// else if time range is less than 1 day and greater than 6 hours, we need to use the `time_series_v4_6hrs` table
	// else we need to use the `time_series_v4_1day` table
	var tableName string
	if end-start <= sixHoursInMilliseconds {
		// adjust the start time to nearest 1 hour
		start = start - (start % (time.Hour.Milliseconds() * 1))
		tableName = distributedTimeSeriesV4
	} else if end-start <= oneDayInMilliseconds {
		// adjust the start time to nearest 6 hours
		start = start - (start % (time.Hour.Milliseconds() * 6))
		tableName = distributedTimeSeriesV46hrs
	} else {
		// adjust the start time to nearest 1 day
		start = start - (start % (time.Hour.Milliseconds() * 24))
		tableName = distributedTimeSeriesV41day
	}

	return start, end, tableName
}

func NewClickHouse(params *ClickHouseParams) (base.Storage, error) {
	l := logrus.WithField("component", "clickhouse")

	options, err := clickhouse.ParseDSN(params.DSN)
	if err != nil {
		return nil, err
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
		database:             options.Auth.Database,
		maxTimeSeriesInQuery: params.MaxTimeSeriesInQuery,

		timeSeries:         make(map[string]map[uint64][]prompb.Label, 262144),
		useClickHouseQuery: useClickHouseQuery,
	}

	go func() {
		ctx := pprof.WithLabels(context.TODO(), pprof.Labels("component", "clickhouse_reloader"))
		pprof.SetGoroutineLabels(ctx)
		// ch.runTimeSeriesReloader(ctx)
	}()

	return ch, nil
}

func (ch *clickHouse) runTimeSeriesReloader(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	queryTmpl := `SELECT DISTINCT fingerprint, labels FROM %s.%s WHERE unix_milli >= $1;`
	for {
		timeSeries := make(map[string]map[uint64][]prompb.Label)
		newSeriesCount := 0
		lastLoadedTimeStamp := time.Now().Add(time.Duration(-1) * time.Minute).UnixMilli()

		err := func() error {
			query := fmt.Sprintf(queryTmpl, ch.database, distributedTimeSeriesV4)
			ch.l.Debug("Running reloader query:", query)
			ctx = ch.addClickHouseSettings(ctx)
			rows, err := ch.db.QueryContext(ctx, query, ch.lastLoadedTimeStamp)
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
					timeSeries[metricName] = make(map[uint64][]prompb.Label)
				}
				if _, ok := timeSeries[metricName][f]; !ok {
					timeSeries[metricName][f] = make([]prompb.Label, 0)
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
						ch.timeSeries[metricName] = make(map[uint64][]prompb.Label)
					}
					ch.timeSeries[metricName][fingerprint] = labels
				}
			}
			ch.lastLoadedTimeStamp = lastLoadedTimeStamp
			ch.timeSeriesRW.Unlock()
			ch.l.Debugf("Loaded %d new time series", newSeriesCount)
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

func (ch *clickHouse) getLogComment(ctx context.Context) string {
	// Get the key-value pairs from context for log comment
	kv := ctx.Value("log_comment")
	if kv == nil {
		return ""
	}

	logCommentKVs, ok := kv.(map[string]string)
	if !ok {
		return ""
	}

	logComment, _ := json.Marshal(logCommentKVs)

	return string(logComment)
}

func (ch *clickHouse) addClickHouseSettings(ctx context.Context) context.Context {
	settings := clickhouse.Settings{}

	logComment := ch.getLogComment(ctx)
	if logComment != "" {
		settings["log_comment"] = logComment
	}

	if os.Getenv("ClickHouseMaxExecutionTime") != "" {
		settings["max_execution_time"] = os.Getenv("ClickHouseMaxExecutionTime")
	}

	if os.Getenv("ClickHouseMaxExecutionTimeLeaf") != "" {
		settings["max_execution_time_leaf"] = os.Getenv("ClickHouseMaxExecutionTimeLeaf")
	}

	if os.Getenv("ClickHouseTimeoutBeforeCheckingExecutionSpeed") != "" {
		settings["timeout_before_checking_execution_speed"] = os.Getenv("ClickHouseTimeoutBeforeCheckingExecutionSpeed")
	}

	if os.Getenv("ClickHouseMaxBytesToRead") != "" {
		settings["max_bytes_to_read"] = os.Getenv("ClickHouseMaxBytesToRead")
	}

	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(settings))
	return ctx
}

func (ch *clickHouse) scanSamples(rows *sql.Rows, fingerprints map[uint64][]prompb.Label) ([]*prompb.TimeSeries, error) {
	// scan results
	var res []*prompb.TimeSeries
	var ts *prompb.TimeSeries
	var fingerprint, prevFingerprint uint64
	var timestampMs int64
	var value float64
	var metricName string

	totalFingerprintCount := len(fingerprints)
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

			labels := fingerprints[fingerprint]
			ts = &prompb.TimeSeries{
				Labels: labels,
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

	ch.l.Infof("Used %d time series from %d time series returned by query", usedFingerprintCount, totalFingerprintCount)

	if err := rows.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	return res, nil
}

func (ch *clickHouse) querySamples(
	ctx context.Context,
	start int64,
	end int64,
	fingerprints map[uint64][]prompb.Label,
	metricName string,
	subQuery string,
	args []interface{},
) ([]*prompb.TimeSeries, error) {

	argCount := len(args)

	query := fmt.Sprintf(`
		SELECT metric_name, fingerprint, unix_milli, value
			FROM %s.%s
			WHERE metric_name = $1 AND fingerprint GLOBAL IN (%s) AND unix_milli >= $%s AND unix_milli <= $%s ORDER BY fingerprint, unix_milli;`,
		ch.database, distributedSamplesV4, subQuery, strconv.Itoa(argCount+2), strconv.Itoa(argCount+3))
	query = strings.TrimSpace(query)

	ch.l.Debugf("Running query : %s", query)

	allArgs := append([]interface{}{metricName}, args...)
	allArgs = append(allArgs, start, end)

	// run query
	ctx = ch.addClickHouseSettings(ctx)
	rows, err := ch.db.QueryContext(ctx, query, allArgs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	return ch.scanSamples(rows, fingerprints)
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
			ch.l.Errorf("convertReadRequest: unexpected matcher %d", m.Type)
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
	var selectString string = "fingerprint, any(labels)"
	if subQuery {
		argCount = 1
		selectString = "fingerprint"
	}

	start, end, tableName := which(query.StartTimestampMs, query.EndTimestampMs)

	var args []interface{}
	conditions = append(conditions, fmt.Sprintf("metric_name = $%d", argCount+1))
	conditions = append(conditions, "temporality IN ['Cumulative', 'Unspecified']")
	conditions = append(conditions, fmt.Sprintf("unix_milli >= %d AND unix_milli < %d", start, end))
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

	clickHouseQuery = fmt.Sprintf(`SELECT %s FROM %s.%s WHERE %s GROUP BY fingerprint`, selectString, ch.database, tableName, whereClause)

	return clickHouseQuery, args, nil
}

func (ch *clickHouse) fingerprintsForQuery(ctx context.Context, query string, args []interface{}) (map[uint64][]prompb.Label, error) {
	// run query
	ctx = ch.addClickHouseSettings(ctx)
	rows, err := ch.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// scan results
	var fingerprint uint64
	var b []byte
	fingerprints := make(map[uint64][]prompb.Label)
	for rows.Next() {
		if err = rows.Scan(&fingerprint, &b); err != nil {
			return nil, err
		}

		labels, _, err := unmarshalLabels(b)

		if err != nil {
			return nil, err
		}
		fingerprints[fingerprint] = labels
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

	var fingerprints map[uint64][]prompb.Label
	var err error

	clickHouseQuery, args, err := ch.prepareClickHouseQuery(query, metricName, false)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	fingerprints, err = ch.fingerprintsForQuery(ctx, clickHouseQuery, args)
	if err != nil {
		return nil, err
	}
	ch.l.Debugf("fingerprintsForQuery took %s, num fingerprints: %d", time.Since(start), len(fingerprints))
	subQuery, args, err := ch.prepareClickHouseQuery(query, metricName, true)
	if err != nil {
		return nil, err
	}

	res := new(prompb.QueryResult)
	if len(fingerprints) == 0 {
		return res, nil
	}

	ts, err := ch.querySamples(ctx, int64(query.StartTimestampMs), int64(query.EndTimestampMs), fingerprints, metricName, subQuery, args)

	if err != nil {
		return nil, err
	}
	res.Timeseries = ts

	return res, nil
}
