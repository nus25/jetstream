package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/cockroachdb/pebble"
	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
	"github.com/nus25/jetstream-proxy/pkg/models"
	"github.com/nus25/jetstream-proxy/pkg/monotonic"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
)

// Consumer is the consumer of the firehose
type Consumer struct {
	Host              string
	Progress          *Progress
	Emit              func(context.Context, *jetstream.Event, []byte, []byte) error
	UncompressedDB    *pebble.DB
	CompressedDB      *pebble.DB
	encoder           *zstd.Encoder
	EventTTL          time.Duration
	logger            *slog.Logger
	clock             *monotonic.Clock
	buf               chan *jetstream.Event
	sequencerShutdown chan chan struct{}

	sequenced prometheus.Counter
	persisted prometheus.Counter
	emitted   prometheus.Counter

	MaxMsgSizeBytes uint32 // instead of using the client's max message size option, handle it in the consumer
}

var tracer = otel.Tracer("consumer")

// NewConsumer creates a new consumer
func NewConsumer(
	ctx context.Context,
	logger *slog.Logger,
	host string,
	dataDir string,
	eventTTL time.Duration,
	emit func(context.Context, *jetstream.Event, []byte, []byte) error,
) (*Consumer, error) {
	uDBPath := dataDir + "/jetstream.uncompressed.db"
	uDB, err := pebble.Open(uDBPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	cDBPath := dataDir + "/jetstream.compressed.db"
	cDB, err := pebble.Open(cDBPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	log := logger.With("component", "consumer")

	clock, err := monotonic.NewClock(time.Microsecond)
	if err != nil {
		return nil, fmt.Errorf("failed to create clock: %w", err)
	}

	// Create a zstd encoder using the dictionary and a window size of 128KiB
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderDict(models.ZSTDDictionary), zstd.WithWindowSize(1<<17), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	c := Consumer{
		Host: host,
		Progress: &Progress{
			LastSeq: ^uint64(0),
		},
		EventTTL:          eventTTL,
		Emit:              emit,
		UncompressedDB:    uDB,
		CompressedDB:      cDB,
		encoder:           encoder,
		logger:            log,
		clock:             clock,
		buf:               make(chan *jetstream.Event, 10_000),
		sequencerShutdown: make(chan chan struct{}),

		sequenced: eventsSequencedCounter.WithLabelValues(host),
		persisted: eventsPersistedCounter.WithLabelValues(host),
		emitted:   eventsEmittedCounter.WithLabelValues(host),
	}

	// Check to see if the cursor exists
	err = c.ReadCursor(ctx)
	if err != nil {
		log.Warn("previous cursor not found, starting from live", "error", err)
	}

	// Start the sequencer
	if err := c.RunSequencer(ctx); err != nil {
		return nil, fmt.Errorf("failed to start sequencer: %w", err)
	}

	return &c, nil
}

func (c *Consumer) RunSequencer(ctx context.Context) error {
	log := c.logger.With("component", "sequencer")

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info("shutting down sequencer on context completion")
				return
			case s := <-c.sequencerShutdown:
				log.Info("shutting down sequencer on shutdown signal")
				s <- struct{}{}
				return
			case e := <-c.buf:
				// Assign a time_us to the event
				e.TimeUS = c.clock.Now()
				c.sequenced.Inc()

				// Serialize the event as JSON
				asJSON, err := json.Marshal(e)
				if err != nil {
					log.Error("failed to marshal event", "error", err)
					return
				}

				// Check event size instead of relying on the client's max message size option
				if c.MaxMsgSizeBytes > 0 && uint32(len(asJSON)) > c.MaxMsgSizeBytes {
					log.Info("event exceeds max message size", "size", len(asJSON), "max", c.MaxMsgSizeBytes)
					return
				}

				// Compress the serialized JSON using zstd
				compBytes := c.encoder.EncodeAll(asJSON, nil)

				// Persist the event to the uncompressed and compressed DBs
				if err := c.PersistEvent(ctx, e, asJSON, compBytes); err != nil {
					log.Error("failed to persist event", "error", err)
					return
				}
				c.persisted.Inc()

				// Emit the event to subscribers
				if err := c.Emit(ctx, e, asJSON, compBytes); err != nil {
					log.Error("failed to emit event", "error", err)
				}
				c.emitted.Inc()
			}
		}
	}()

	return nil
}

func (c *Consumer) Shutdown() {
	shutdownTimeout := time.After(10 * time.Second)
	shutdown := make(chan struct{})
	c.sequencerShutdown <- shutdown

	select {
	case <-shutdownTimeout:
		c.logger.Warn("sequencer shutdown timed out")
	case <-shutdown:
		c.logger.Info("sequencer shutdown complete")
	}
}

func (c *Consumer) AddEvent(event *jetstream.Event) {
	c.buf <- event
}
