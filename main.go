package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"sqslite/pkg/httputil"
	"sqslite/pkg/sqslite"
)

func main() {
	slog.SetDefault(
		slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: false,
		})),
	)
	server := sqslite.NewServer(
		sqslite.OptBaseQueueURL(
			fmt.Sprintf("http://sqslite.%s.local:4566", "us-west-2"),
		),
	)
	httpSrv := &http.Server{
		Addr:    ":4566",
		Handler: httputil.LoggedHandler(server),
	}
	group, groupCtx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-groupCtx.Done():
				return nil
			case <-t.C:
				printServerStats(server)
			}
		}
	})
	group.Go(func() error {
		slog.Info("server listening", slog.String("addr", ":4566"))
		return httpSrv.ListenAndServe()
	})
	group.Go(func() error {
		ctx, done := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer done()
		select {
		case <-ctx.Done():
			shutdownContext, shutdownComplete := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownComplete()
			return httpSrv.Shutdown(shutdownContext)
		}
	})
	if err := group.Wait(); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Info("server exiting with error", slog.Any("err", err))
			os.Exit(1)
		}
	}
}

func printServerStats(server *sqslite.Server) {
	for q := range server.Queues() {
		stats := q.Stats()
		slog.Info(
			"queue stats",
			slog.String("queue", q.Name),
			slog.Int64("num_messages", stats.NumMessages),
			slog.Int64("num_messages_ready", stats.NumMessagesReady),
			slog.Int64("num_messages_outstanding", stats.NumMessagesOutstanding),
			slog.Int64("num_messages_delayed", stats.NumMessagesDelayed),
		)
	}
}
