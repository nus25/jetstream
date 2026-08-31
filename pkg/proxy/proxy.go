package proxy

// jet stream proxy custormize code
// nus

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/nus25/jetstream-proxy/pkg/consumer"
)

type Handler struct {
	LastSeq  uint64
	Consumer *consumer.Consumer
	NextMet  int64
}
type ClientConfig struct {
	Host              string
	WantedDids        []string
	WantedCollections []string
	MaxMsgSizeBytes   uint32
	ZstdCompression   bool
}

func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Host:              "localhost:6008",
		WantedDids:        []string{},
		WantedCollections: []string{},
		MaxMsgSizeBytes:   0,
		ZstdCompression:   true,
	}
}

// jetstreamエンドポイントと通信を行う。
// 通信エラーの場合、接続を閉じてエラーを返す。
func HandleRepoStream(ctx context.Context, config *ClientConfig, seq uint64, maxEventAge time.Duration, c *consumer.Consumer, logger *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Recovered from panic", "error", r)
			err = fmt.Errorf("Recovered from: %v", r)
		}
	}()
	h := &Handler{
		LastSeq:  ^uint64(0),
		Consumer: c,
		NextMet:  -1,
	}
	host := strings.TrimPrefix(config.Host, "wss://")
	host = strings.TrimSuffix(host, "/subscribe")
	logger.Info("Connecting to jetstream", "host", host)

	options := []jetstream.Option{
		jetstream.WithLogger(logger),
	}
	if len(config.WantedCollections) > 0 {
		options = append(options, jetstream.WithCollections(config.WantedCollections))
		logger.Info("Filtering by collections", "collections", strings.Join(config.WantedCollections, ","))
	}
	if len(config.WantedDids) > 0 {
		options = append(options, jetstream.WithDIDs(config.WantedDids))
		logger.Info("Filtering by DIDs", "dids", strings.Join(config.WantedDids, ","))
	}
	if seq != consumer.UnsetSeq {
		options = append(options, jetstream.WithLiveCursor(seq))
		logger.Info("Starting from seq", "seq", seq)
	} else {
		logger.Info("Starting from live tail")
	}
	if config.MaxMsgSizeBytes > 0 {
		//No option proviced by client. handle in consumer instead.
		c.MaxMsgSizeBytes = config.MaxMsgSizeBytes
		//options = append(options, jetstream.WithMaxMsgSize(config.MaxMsgSizeBytes))
		logger.Info("Setting max message size", "max_msg_size_bytes", config.MaxMsgSizeBytes)
	}
	if config.ZstdCompression {
		options = append(options, jetstream.WithZstdCompression(config.ZstdCompression))
		logger.Info("Enabling zstd compression")
	}

	logger.Info("Max event age set", "max_event_age", maxEventAge)

	client, err := jetstream.Subscribe(
		host, // "jetstream.us-east.bsky.network"
		options...,
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		logger.Info("Closing client")
		e := client.Close()
		if e != nil {
			logger.Error("Error closing client", "error", e)
		}
	}()

	go func() {
		for batch, err := range client.Events(ctx) {
			if err != nil {
				continue
			}

			for _, evt := range batch.Events() {
				// commit, handle,identity ,identity,info,migrate,tombstone,labelsまとめて処理
				//JSONのcursorフィールドはSeqとして扱われる
				h.LastSeq = evt.Seq
				h.Consumer.Progress.Update(evt.Seq, time.Now())

				// skip expired commit events
				expiredDate := time.Now().Add(-maxEventAge).Format(time.RFC3339)
				if maxEventAge > 0 && evt.Commit != nil && evt.Commit.Record["createdAt"] != nil {
					if evt.Commit.Record["createdAt"].(string) < expiredDate {
						jetstreamSkippedEvents.Inc()
						continue
					}
				}

				h.Consumer.AddEvent(&evt)
				if h.NextMet < evt.TimeUS && evt.Identity != nil {
					t, _ := time.Parse(time.RFC3339, evt.Identity.Time)
					jetstreamDelay.Set(time.Since(t).Seconds())
					//5秒に一回くらい更新
					h.NextMet = evt.TimeUS + 5e+6
				}
			}
		}
	}()

	<-ctx.Done()
	logger.Info("repo stream closed")
	return ctx.Err()
}
