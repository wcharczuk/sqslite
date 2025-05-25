package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"sqslite/pkg/httputil"
	"sqslite/pkg/sqslite"
)

var (
	flagBindAddr            = pflag.String("bind-addr", ":4566", "The server bind address")
	flagShutdownGracePeriod = pflag.Duration("shutdown-grace-period", 30*time.Second, "The server shutdown grace period")
)

func main() {
	flag.Parse()
	slog.SetDefault(
		slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: false,
		})),
	)

	server := sqslite.NewServer(
		sqslite.OptBaseQueueURL(
			fmt.Sprintf("http://sqslite.%s.local", "us-west-2"),
		),
	)
	httpSrv := &http.Server{
		Addr:    *flagBindAddr,
		Handler: httputil.LoggedHandler(server),
	}
	group, groupCtx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		t := time.NewTicker(2 * time.Second)
		prevTimestamp := time.Now()
		defer t.Stop()
		prevStats := make(map[string]sqslite.QueueStats)
		for {
			select {
			case <-groupCtx.Done():
				return nil
			case <-t.C:
				prevStats = printServerStats(server, time.Since(prevTimestamp), prevStats)
				prevTimestamp = time.Now()
			}
		}
	})
	group.Go(func() error {
		slog.Info("server listening", slog.String("addr", *flagBindAddr))
		return httpSrv.ListenAndServe()
	})
	group.Go(func() error {
		ctx, done := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer done()
		select {
		case <-groupCtx.Done():
			shutdownContext, shutdownComplete := context.WithTimeout(context.Background(), *flagShutdownGracePeriod)
			defer shutdownComplete()
			return httpSrv.Shutdown(shutdownContext)
		case <-ctx.Done():
			shutdownContext, shutdownComplete := context.WithTimeout(context.Background(), *flagShutdownGracePeriod)
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

func printServerStats(server *sqslite.Server, elapsed time.Duration, prev map[string]sqslite.QueueStats) map[string]sqslite.QueueStats {
	var elapsedSeconds float64 = float64(elapsed) / float64(time.Second)
	newStats := make(map[string]sqslite.QueueStats)
	for q := range server.Queues() {
		prevStats, _ := prev[q.Name]
		stats := q.Stats()
		changeMessagesSent := float64(stats.TotalMessagesSent - prevStats.TotalMessagesSent)
		changeMessagesReceived := float64(stats.TotalMessagesReceived - prevStats.TotalMessagesReceived)
		changeMessagesDeleted := float64(stats.TotalMessagesDeleted - prevStats.TotalMessagesDeleted)
		slog.Info(
			"queue stats",
			slog.String("queue", q.Name),
			slog.Int64("num_messages", stats.NumMessages),
			slog.Int64("num_messages_ready", stats.NumMessagesReady),
			slog.Int64("num_messages_outstanding", stats.NumMessagesOutstanding),
			slog.Int64("num_messages_delayed", stats.NumMessagesDelayed),
			slog.String("send_rate", fmt.Sprintf("%02f/sec", changeMessagesSent/elapsedSeconds)),
			slog.String("receive_rate", fmt.Sprintf("%02f/sec", changeMessagesReceived/elapsedSeconds)),
			slog.String("delete_rate", fmt.Sprintf("%02f/sec", changeMessagesDeleted/elapsedSeconds)),
		)
		newStats[q.Name] = stats
	}
	return newStats
}
