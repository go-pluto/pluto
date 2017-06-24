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

type PlutoMetrics struct {
	Distributor *DistrobutorMetrics
}

type DistrobutorMetrics struct {
	Logins  metrics.Counter
	Logouts metrics.Counter
}

func NewPlutoMetrics(distributorAddr string) *PlutoMetrics {

	m := &PlutoMetrics{}

	if distributorAddr == "" {
		m.Distributor = &DistrobutorMetrics{
			Logins:  discard.NewCounter(),
			Logouts: discard.NewCounter(),
		}
	} else {
		m.Distributor = &DistrobutorMetrics{
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
