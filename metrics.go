package main

import (
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/go-kit/kit/metrics/prometheus"
	"github.com/go-pluto/pluto/distributor"
	prom "github.com/prometheus/client_golang/prometheus"
)

// PlutoMetrics wraps all metrics for Pluto into one struct.
type PlutoMetrics struct {
	Distributor *distributor.Metrics
}

// NewPlutoMetrics returns Prometheus metrics when addr isn't
// an empty string. Otherwise discard metrics are returned.
func NewPlutoMetrics(distributorAddr string) *PlutoMetrics {

	m := &PlutoMetrics{}

	if distributorAddr == "" {
		m.Distributor = &distributor.Metrics{
			Commands:    discard.NewCounter(),
			Connections: discard.NewCounter(),
		}
	} else {
		m.Distributor = &distributor.Metrics{
			Commands: prometheus.NewCounterFrom(
				prom.CounterOpts{
					Namespace: "pluto",
					Subsystem: "distributor",
					Name:      "commands_total",
					Help:      "Number of commands",
				}, []string{"command", "status"},
			),
			Connections: prometheus.NewCounterFrom(
				prom.CounterOpts{
					Namespace: "pluto",
					Subsystem: "distributor",
					Name:      "connections_total",
					Help:      "Number of connections opened to pluto",
				}, nil,
			),
		}
	}

	return m
}

func runPromHTTP(logger log.Logger, addr string) {

	if addr == "" {
		level.Debug(logger).Log("msg", "prometheus addr is empty, not exposing prometheus metrics")
		return
	}

	http.Handle("/metrics", prom.UninstrumentedHandler())

	level.Info(logger).Log("msg", "prometheus handler listening", "addr", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		level.Warn(logger).Log("msg", "failed to serve prometheus metrics", "err", err)
	}
}
