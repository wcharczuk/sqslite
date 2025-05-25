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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"sqslite/pkg/httputil"
	"sqslite/pkg/slant"
	"sqslite/pkg/sqslite"
)

var (
	flagBindAddr            = pflag.String("bind-addr", ":4566", "The server bind address")
	flagShutdownGracePeriod = pflag.Duration("shutdown-grace-period", 30*time.Second, "The server shutdown grace period")
	flagLogFormat           = pflag.String("log-format", "json", "The log format (json|text)")
	flagLogLevel            = pflag.String("log-level", slog.LevelInfo.String(),
		fmt.Sprintf(
			"The log level (%s>%s>%s>%s) (not case sensitive, from least to most restrictive)",
			slog.LevelDebug.String(),
			slog.LevelInfo.String(),
			slog.LevelWarn.String(),
			slog.LevelError.String(),
		))
)

func main() {
	pflag.Parse()

	//
	// logger setup
	//
	logLeveler := new(slog.LevelVar)
	if err := logLeveler.UnmarshalText([]byte(*flagLogLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(logLeveler.Level().String())
	switch *flagLogFormat {
	case "json":
		slog.SetDefault(
			slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
				AddSource: false,
				Level:     logLeveler,
			})),
		)
	default:
	case "text":
		slog.SetDefault(
			slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				AddSource: false,
				Level:     logLeveler,
			})),
		)
		slant.Print(os.Stdout, "sqslite")
	}
	slog.Info("using log level", slog.String("log_level", logLeveler.Level().String()))

	//
	// server setup
	//
	server := sqslite.NewServer(
		sqslite.OptBaseQueueURL(
			fmt.Sprintf("http://sqslite.%s.local", "us-west-2"),
		),
	)

	//
	// create the default queue
	//
	defaultQueue, _ := sqslite.NewQueueFromCreateQueueInput(server.BaseQueueURL(), &sqs.CreateQueueInput{
		QueueName: aws.String("default"),
	})
	_ = server.Queues().CreateQueue(context.Background(), defaultQueue)
	slog.Info("created default queue with url", slog.String("queue_url", defaultQueue.URL))

	httpSrv := &http.Server{
		Addr:    *flagBindAddr,
		Handler: httputil.LoggedHandler(server),
	}
	group, groupCtx := errgroup.WithContext(context.Background())
	group.Go(func() error {
		t := time.NewTicker(10 * time.Second)
		prevTimestamp := time.Now()
		defer t.Stop()
		prevStats := make(map[string]sqslite.QueueStats)
		for {
			select {
			case <-groupCtx.Done():
				return nil
			case <-t.C:
				prevStats = printStatistics(server, time.Since(prevTimestamp), prevStats)
				prevTimestamp = time.Now()
			}
		}
	})
	group.Go(func() error {
		slog.Info("server listening", slog.String("addr", *flagBindAddr))
		return httpSrv.ListenAndServe()
	})
	group.Go(func() error {
		updateLogLevel := make(chan os.Signal, 1)
		updateLogLevelSignals := []os.Signal{
			syscall.SIGUSR1,
			syscall.SIGUSR2,
		}
		signal.Notify(updateLogLevel, updateLogLevelSignals...)
		defer signal.Reset(updateLogLevelSignals...)
		increaseLogLevel := func() {
			switch logLeveler.Level() {
			case slog.LevelInfo:
				logLeveler.Set(slog.LevelDebug)
			case slog.LevelWarn:
				logLeveler.Set(slog.LevelInfo)
			case slog.LevelError:
				logLeveler.Set(slog.LevelWarn)
			}
		}
		decreaseLogLevel := func() {
			switch logLeveler.Level() {
			case slog.LevelDebug:
				logLeveler.Set(slog.LevelInfo)
			case slog.LevelInfo:
				logLeveler.Set(slog.LevelWarn)
			case slog.LevelWarn:
				logLeveler.Set(slog.LevelError)
			}
		}
		for {
			select {
			case <-groupCtx.Done():
				return nil
			case sig := <-updateLogLevel:
				switch sig {
				case syscall.SIGUSR1:
					decreaseLogLevel()
				case syscall.SIGUSR2:
					increaseLogLevel()
				}
				continue
			}
		}
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

func printStatistics(server *sqslite.Server, elapsed time.Duration, prev map[string]sqslite.QueueStats) map[string]sqslite.QueueStats {
	var elapsedSeconds float64 = float64(elapsed) / float64(time.Second)
	newStats := make(map[string]sqslite.QueueStats)
	for q := range server.EachQueue() {
		prevStats, _ := prev[q.Name]
		stats := q.Stats()
		changeMessagesSent := float64(stats.TotalMessagesSent - prevStats.TotalMessagesSent)
		changeMessagesReceived := float64(stats.TotalMessagesReceived - prevStats.TotalMessagesReceived)
		changeMessagesChangedVisiblity := float64(stats.TotalMessagesChangedVisibility - prevStats.TotalMessagesChangedVisibility)
		changeMessagesDeleted := float64(stats.TotalMessagesDeleted - prevStats.TotalMessagesDeleted)
		changeMessagesPurged := float64(stats.TotalMessagesPurged - prevStats.TotalMessagesPurged)
		slog.Debug(
			"statistics",
			slog.String("queue", q.Name),
			slog.Int64("num_messages", stats.NumMessages),
			slog.Int64("num_messages_ready", stats.NumMessagesReady),
			slog.Int64("num_messages_outstanding", stats.NumMessagesOutstanding),
			slog.Int64("num_messages_delayed", stats.NumMessagesDelayed),
			slog.String("sent_rate", fmt.Sprintf("%0.2f/sec", changeMessagesSent/elapsedSeconds)),
			slog.String("received_rate", fmt.Sprintf("%0.2f/sec", changeMessagesReceived/elapsedSeconds)),
			slog.String("changed_visibility_rate", fmt.Sprintf("%0.2f/sec", changeMessagesChangedVisiblity/elapsedSeconds)),
			slog.String("deleted_rate", fmt.Sprintf("%0.2f/sec", changeMessagesDeleted/elapsedSeconds)),
			slog.String("purged_rate", fmt.Sprintf("%0.2f/sec", changeMessagesPurged/elapsedSeconds)),
		)
		newStats[q.Name] = stats
	}
	return newStats
}
