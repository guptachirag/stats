package stats

import (
	"errors"
	"fmt"
	"net/http"
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
	count int64
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
	FlushDuration time.Duration // stats flush duration in milli seconds
}

type Stats struct {
	qpsStats       map[string]*qps
	latencyStats   map[string]*latency
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
		qpsStats:       make(map[string]*qps),
		latencyStats:   make(map[string]*latency),
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
	ss.latencyQueue <- &latency{stat: stat, count: 1, delta: delta}
}

func (ss *Stats) processQPSStat(q *qps) {
	s, ok := ss.qpsStats[q.stat]
	if ok {
		s.count = s.count + q.count
	} else {
		ss.qpsStats[q.stat] = q
	}
}

func (ss *Stats) processLatencyStat(l *latency) {
	s, ok := ss.latencyStats[l.stat]
	if ok {
		s.delta = s.delta + l.delta
		s.count = s.count + l.count
	} else {
		ss.latencyStats[l.stat] = l
	}
}

func (ss *Stats) updateStats(quit bool) {
	if quit {
		ss.writeStatsToInfluxDB(ss.qpsStats, ss.latencyStats)
		ss.influxDBClient.Close()
	} else {
		go ss.writeStatsToInfluxDB(ss.qpsStats, ss.latencyStats)
		ss.qpsStats = make(map[string]*qps)
		ss.latencyStats = make(map[string]*latency)
	}
}

func (ss *Stats) writeStatsToInfluxDB(qpsStats map[string]*qps, latencyStats map[string]*latency) {
	bpts, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  ss.influxDBConfig.DB,
		Precision: ss.influxDBConfig.Precision,
	})
	if err != nil {
		panic(err)
	}

	for stat, q := range qpsStats {
		fields := make(map[string]interface{})
		fields["value"] = q.count / int64(ss.statsConfig.FlushDuration/time.Second)
		pt, err := influxdb.NewPoint(stat, ss.tags, fields, time.Now())
		if err != nil {
			panic(err)
		}
		bpts.AddPoint(pt)
	}

	for stat, l := range latencyStats {
		fields := make(map[string]interface{})
		fields["value"] = l.delta.Nanoseconds() / l.count
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
