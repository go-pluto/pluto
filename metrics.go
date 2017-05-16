package main

import (
	"net/http"

	"github.com/go-kit/kit/metrics/discard"
	"github.com/go-kit/kit/metrics/prometheus"
	"github.com/numbleroot/pluto/imap"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsNamespace = "pluto"

// PlutoMetrics combines all metrics across pluto into one struct
// for easier initialization and passing.
type PlutoMetrics struct {
	Distributor imap.DistributorMetrics
}

// NewPlutoMetrics starts a http server on the given address
// and returns PlutoMetrics.
func NewPlutoMetrics(addr string) *PlutoMetrics {

	if addr == "" {
		return &PlutoMetrics{
			Distributor: imap.DistributorMetrics{
				Commands: discard.NewCounter(),
			},
		}
	}

	// Start prometheus in a goroutine running concurrently in the background
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(addr, nil)
	}()

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
