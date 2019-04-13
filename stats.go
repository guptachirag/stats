package stats

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	influxdb "github.com/influxdata/influxdb/client/v2"
)

type qps struct {
	stat  string
	count int64
}

type latency struct {
	stat  string
	delta time.Duration
}

type InfluxDBConfig struct {
	Host      string
	Port      int
	User      string
	Password  string
	DB        string
	Precision string
}

type StatsConfig struct {
	QueueSize     int
	FlushDuration time.Duration
}

type Stats struct {
	qpsStats       map[string]int64
	latencyStats   map[string][]time.Duration
	qpsQueue       chan *qps
	latencyQueue   chan *latency
	quit           chan bool
	wg             *sync.WaitGroup
	ticker         *time.Ticker
	influxDBClient influxdb.Client
	influxDBConfig *InfluxDBConfig
	statsConfig    *StatsConfig
	tags           map[string]string
}

func NewStats(sc *StatsConfig, ic *InfluxDBConfig, tags map[string]string) (*Stats, error) {
	if sc.FlushDuration == 0 {
		sc.FlushDuration = 10 * time.Second
	}
	if sc.QueueSize == 0 {
		sc.QueueSize = 1000
	}

	influxDBClient, err := influxdb.NewHTTPClient(
		influxdb.HTTPConfig{
			Addr:     fmt.Sprintf("http://%s:%d", ic.Host, ic.Port),
			Username: ic.User,
			Password: ic.Password,
		})
	if err != nil {
		return nil, errors.New("Error initializing influx db client, " + err.Error())
	}

	ss := &Stats{
		qpsStats:       make(map[string]int64),
		latencyStats:   make(map[string][]time.Duration),
		qpsQueue:       make(chan *qps, sc.QueueSize),
		latencyQueue:   make(chan *latency, sc.QueueSize),
		quit:           make(chan bool),
		wg:             &sync.WaitGroup{},
		ticker:         time.NewTicker(sc.FlushDuration),
		influxDBClient: influxDBClient,
		influxDBConfig: ic,
		statsConfig:    sc,
		tags:           tags,
	}

	ss.wg.Add(1)
	go func() {
		defer ss.wg.Done()
		for {
			select {
			case <-ss.ticker.C:
				ss.updateStats(false)
			case q := <-ss.qpsQueue:
				ss.processQPSStat(q)
			case l := <-ss.latencyQueue:
				ss.processLatencyStat(l)
			case <-ss.quit:
				for q := range ss.qpsQueue {
					ss.processQPSStat(q)
				}
				for l := range ss.latencyQueue {
					ss.processLatencyStat(l)
				}
				ss.updateStats(true)
				return
			}
		}
	}()
	return ss, nil
}

func getStatNamesForRequest(r *http.Request) (string, string) {
	statName := "home"
	path := r.URL.EscapedPath()
	if path != "/" && path != "" {
		if path[0] == '/' {
			path = path[1:len(path)]
		}
		if path[len(path)-1] == '/' {
			path = path[0 : len(path)-1]
		}
		statName = strings.Replace(path, "/", "_", -1)
	}
	countStatName := statName + "_count"
	latencyStatName := statName + "_latency"
	return countStatName, latencyStatName
}

func (ss *Stats) ServeHTTP(rw http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	startTime := time.Now()
	countStatName, latencyStatName := getStatNamesForRequest(r)
	ss.Incr(countStatName, 1)
	next(rw, r)
	ss.Timing(latencyStatName, time.Now().Sub(startTime))
}

func (ss *Stats) Close() {
	ss.quit <- true
	close(ss.quit)
	close(ss.qpsQueue)
	close(ss.latencyQueue)
	ss.wg.Wait()
}

func (ss *Stats) Incr(stat string, count int64) {
	ss.qpsQueue <- &qps{stat: stat, count: count}
}

func (ss *Stats) Timing(stat string, delta time.Duration) {
	ss.latencyQueue <- &latency{stat: stat, delta: delta}
}

func (ss *Stats) processQPSStat(q *qps) {
	count, ok := ss.qpsStats[q.stat]
	if ok {
		q.count = q.count + count
	}
	ss.qpsStats[q.stat] = q.count
}

func (ss *Stats) processLatencyStat(l *latency) {
	durations, ok := ss.latencyStats[l.stat]
	if ok {
		durations = append(durations, l.delta)
		ss.latencyStats[l.stat] = durations
	} else {
		ss.latencyStats[l.stat] = []time.Duration{l.delta}
	}
}

func (ss *Stats) updateStats(quit bool) {
	if quit {
		ss.writeStatsToInfluxDB(ss.qpsStats, ss.latencyStats)
		ss.influxDBClient.Close()
	} else {
		go ss.writeStatsToInfluxDB(ss.qpsStats, ss.latencyStats)
		ss.qpsStats = make(map[string]int64)
		ss.latencyStats = make(map[string][]time.Duration)
	}
}

func (ss *Stats) writeStatsToInfluxDB(qpsStats map[string]int64, latencyStats map[string][]time.Duration) {
	bpts, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  ss.influxDBConfig.DB,
		Precision: ss.influxDBConfig.Precision,
	})
	if err != nil {
		panic(err)
	}

	for stat, count := range qpsStats {
		fields := make(map[string]interface{})
		fields["value"] = count / int64(ss.statsConfig.FlushDuration/time.Second)
		pt, err := influxdb.NewPoint(stat, ss.tags, fields, time.Now())
		if err != nil {
			panic(err)
		}
		bpts.AddPoint(pt)
	}

	for stat, durations := range latencyStats {

		sort.Slice(durations, func(i, j int) bool {
			return durations[i] <= durations[j]
		})

		lenDurations := len(durations)
		sumDurations := int64(0)
		for _, duration := range durations {
			sumDurations += duration.Nanoseconds()
		}

		fields := make(map[string]interface{})
		fields["value"] = sumDurations / int64(lenDurations)

		indexP50 := int(float64(lenDurations) * 0.5)
		fields["value_p50"] = durations[indexP50].Nanoseconds()

		indexP60 := int(float64(lenDurations) * 0.6)
		fields["value_p60"] = durations[indexP60].Nanoseconds()

		indexP70 := int(float64(lenDurations) * 0.7)
		fields["value_p70"] = durations[indexP70].Nanoseconds()

		indexP80 := int(float64(lenDurations) * 0.8)
		fields["value_p80"] = durations[indexP80].Nanoseconds()

		indexP90 := int(float64(lenDurations) * 0.9)
		fields["value_p90"] = durations[indexP90].Nanoseconds()

		indexP95 := int(float64(lenDurations) * 0.95)
		fields["value_p95"] = durations[indexP95].Nanoseconds()

		indexP98 := int(float64(lenDurations) * 0.98)
		fields["value_p98"] = durations[indexP98].Nanoseconds()

		indexP99 := int(float64(lenDurations) * 0.99)
		fields["value_p99"] = durations[indexP99].Nanoseconds()

		indexP999 := int(float64(lenDurations) * 0.999)
		fields["value_p999"] = durations[indexP999].Nanoseconds()

		indexP9999 := int(float64(lenDurations) * 0.9999)
		fields["value_p9999"] = durations[indexP9999].Nanoseconds()

		pt, err := influxdb.NewPoint(stat, ss.tags, fields, time.Now())
		if err != nil {
			panic(err)
		}
		bpts.AddPoint(pt)
	}

	if err := ss.influxDBClient.Write(bpts); err != nil {
		panic(err)
	}
}
