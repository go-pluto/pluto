package main

import (
	"github.com/go-kit/kit/metrics/prometheus"
	"github.com/numbleroot/pluto/imap"
	prom "github.com/prometheus/client_golang/prometheus"
)

const metricsNamespace = "pluto"

type PlutoMetrics struct {
	Distributor imap.DistributorMetrics
}

func NewPrometheusMetrics() *PlutoMetrics {
	return &PlutoMetrics{
		Distributor: imap.DistributorMetrics{
			Commands: prometheus.NewCounterFrom(prom.CounterOpts{
				Namespace: metricsNamespace,
				Subsystem: "distributor",
				Name:      "received_commands_total",
				Help:      "Number of received commands in total by their command type",
			}, []string{"command"}),
		},
	}
}
