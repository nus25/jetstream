package consumer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var eventsSequencedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "consumer_events_sequenced_total",
	Help: "The total number of events sequenced",
}, []string{"socket_url"})

var eventsPersistedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "consumer_events_persisted_total",
	Help: "The total number of events persisted",
}, []string{"socket_url"})

var eventsEmittedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "consumer_events_emitted_total",
	Help: "The total number of events emitted",
}, []string{"socket_url"})
