package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPrometheusMetrics(t *testing.T) {
	metrics := NewPlutoMetrics("")
	assert.NotNil(t, metrics.Distributor.Commands)

	metrics = NewPlutoMetrics(":9099")
	assert.NotNil(t, metrics.Distributor.Commands)
}
