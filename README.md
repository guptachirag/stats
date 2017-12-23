# stats
A Golang middleware to manage qps and latency stats and asynchronously write to influxdb

#### Installation
```go get "github.com/guptachirag/stats"```

#### Usage
```go
package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/guptachirag/stats"
	"github.com/urfave/negroni"
)

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte("Home"))
	})
	mux.HandleFunc("/about", func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte("About"))
	})

	n := negroni.Classic()

	ss, err := stats.NewStats(
		&stats.StatsConfig{
			QueueSize:     1000,
			FlushDuration: time.Second,
		},
		&stats.InfluxDBConfig{
			Host:      "127.0.0.1",
			Port:      8086,
			User:      "root",
			Password:  "root",
			DB:        "server_stats",
			Precision: "ns",
		},
		map[string]string{
			"host": "example.com",
		})
	if err != nil {
		log.Fatal(err)
	}

	n.Use(ss)
	n.UseHandler(mux)

	s := http.Server{
		Addr:    ":8080",
		Handler: n,
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	go func(sigs chan os.Signal) {
		for {
			sig := <-sigs
			switch sig {
			case syscall.SIGTERM:
				if err := s.Shutdown(nil); err != nil {
					if err := s.Close(); err != nil {
						log.Print(err)
						ss.Close() // releases the resources and flush remaining data from channel, if any
						os.Exit(1)
					}
				}
			}
		}
	}(sigs)

	log.Fatal(s.ListenAndServe())
}
```
