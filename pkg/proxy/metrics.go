package proxy

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var jetstreamDelay = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "jetstream_delay_seconds",
	Help: "The current delay of jetstream in seconds",
})

var jetstreamSkippedEvents = promauto.NewCounter(prometheus.CounterOpts{
	Name: "jetstream_skipped_events_total",
	Help: "The total number of jetstream events skipped due to max event age",
})

var jetstreamReceiveErrors = promauto.NewCounter(prometheus.CounterOpts{
	Name: "jetstream_error_total",
	Help: "The total number of errors encountered while receiving jetstream events",
})
