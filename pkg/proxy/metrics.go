package proxy

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	jetstreamDelay = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jetstream_delay_seconds",
		Help: "The current delay of jetstream in seconds",
	})
)
