package main

import (
	"net/http"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/go-kit/kit/metrics/prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
)

// PlutoMetrics wraps all metrics for Pluto into one struct.
type PlutoMetrics struct {
	Distributor *DistributorMetrics
}

// DistributorMetrics has all metrics exposed by the distributor.
type DistributorMetrics struct {
	Logins  metrics.Counter
	Logouts metrics.Counter
}

// NewPlutoMetrics returns prometheus metrics when the addr isn't an empty string.
// Otherwise discard metrics are returned.
func NewPlutoMetrics(distributorAddr string) *PlutoMetrics {

	m := &PlutoMetrics{}

	if distributorAddr == "" {
		m.Distributor = &DistributorMetrics{
			Logins:  discard.NewCounter(),
			Logouts: discard.NewCounter(),
		}
	} else {
		m.Distributor = &DistributorMetrics{
			Logins: prometheus.NewCounterFrom(prom.CounterOpts{
				Namespace: "pluto",
				Subsystem: "distributor",
				Name:      "logins_total",
				Help:      "Number of logins",
			}, nil),
			Logouts: prometheus.NewCounterFrom(prom.CounterOpts{
				Namespace: "pluto",
				Subsystem: "distributor",
				Name:      "logouts_total",
				Help:      "Number of logouts",
			}, nil),
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
