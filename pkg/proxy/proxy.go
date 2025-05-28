package proxy

// jet stream proxy custormize code
// based on  "github.com/bluesky-social/indigo/events"
// nus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/consumer"
	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/labstack/gommon/log"
)

type StreamHeader struct {
	Kind   string `json:"kind"`
	TimeUs int64  `json:"time_us"`
	Did    string `json:"did"`
}

type StreamCommitMessage struct {
	StreamHeader
	Commit struct {
		Operation  string        `json:"operation"`
		Collection string        `json:"collection"`
		Rkey       string        `json:"rkey"`
		Cid        *util.LexLink `json:"cid"`
	} `json:"commit"`
}

// jetstreamエンドポイントと通信を行う。
// 通信エラーの場合、接続を閉じてエラーを返す。
func HandleRepoStream(ctx context.Context, cli *client.Client, cursor *int64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 30秒おきにpingで回線チェック
	go func() {
		t := time.NewTicker(time.Second * 30)
		defer t.Stop()

		for {

			select {
			case <-t.C:

				if err := cli.SendPing(); err != nil {
					log.Warnf("failed to ping: %s", err)
				}
			case <-ctx.Done():
				log.Info("jetstream client closed.")
				return
			}
		}
	}()

	//接続開始
	if err := cli.ConnectAndRead(ctx, cursor); err != nil {
		if errors.Is(err, context.Canceled) {
			// 正常なキャンセルの場合
			return context.Canceled
		}
		if strings.Contains(err.Error(), "use of closed network connection") {
			// 接続が閉じられた場合
			return nil
		}
		// その他の予期せぬエラーの場合
		return fmt.Errorf("failed to connect and read: %w", err)
	}

	return nil
}

type Handler struct {
	LastSeq  int64
	Consumer *consumer.Consumer
	NextMet  int64
}

// jetstreamからのメッセージを受けてクライアント送信用スケジューラーにAddする
func (h *Handler) HandleEvent(ctx context.Context, event *models.Event) error {
	// commit, handle,identity ,identity,info,migrate,tombstone,labelsまとめて処理
	//todo：シーケンスが古い場合は無視
	now := time.Now()
	h.Consumer.Progress.Update(event.TimeUS, now)
	h.Consumer.AddEvent(event)
	if h.NextMet < event.TimeUS && event.Identity != nil {
		t, _ := time.Parse(time.RFC3339, event.Identity.Time)
		jetstreamDelay.Set(time.Since(t).Seconds())
		//5秒に一回くらい更新
		h.NextMet = event.TimeUS + 5e+6
	}
	return nil
}
