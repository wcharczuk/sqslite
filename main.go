package main

import (
	"context"
	"encoding/json"
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
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"github.com/wcharczuk/sqslite/internal/httpz"
	"github.com/wcharczuk/sqslite/internal/slant"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

var (
	flagAWSRegion           = pflag.String("region", "us-west-2", "The AWS region of the server")
	flagBindAddr            = pflag.String("bind-addr", ":4566", "The server bind address")
	flagShutdownGracePeriod = pflag.Duration("shutdown-grace-period", 30*time.Second, "The server shutdown grace period")
	flagLogFormat           = pflag.String("log-format", "text", "The log format (json|text)")
	flagLogLevel            = pflag.String("log-level", slog.LevelInfo.String(),
		fmt.Sprintf(
			"The log level (%s>%s>%s>%s) (not case sensitive, from least to most restrictive)",
			slog.LevelDebug.String(),
			slog.LevelInfo.String(),
			slog.LevelWarn.String(),
			slog.LevelError.String(),
		))
	flagGzip          = pflag.Bool("gzip", false, "If we should enable gzip compression on responses")
	flagSkipStats     = pflag.Bool("skip-stats", false, "If we should skip outputing queue stats every 10 seconds")
	flagStatsInterval = pflag.Duration("stats-interval", 10*time.Second, "The interval in time between printing stats (exclusive with --skip-stats)")
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
	switch *flagLogFormat {
	case "json":
		slog.SetDefault(
			slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
				AddSource: false,
				Level:     logLeveler,
			})),
		)
	case "text":
		slog.SetDefault(
			slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				AddSource: false,
				Level:     logLeveler,
			})),
		)
		slant.Print(os.Stdout, "sqslite")
	default:
		slog.Error("invalid log level", slog.String("log_level", *flagLogLevel))
		os.Exit(1)
	}
	slog.Error("using log level", slog.String("log_level", logLeveler.Level().String()))

	//
	// server setup
	//

	server := sqslite.NewServer()

	//
	// create the default queue(s)
	//

	defaultAuthorization := sqslite.Authorization{
		Region:    sqslite.Some(*flagAWSRegion),
		AccountID: sqslite.DefaultAccountID,
	}
	defaultQueueDLQ, _ := sqslite.NewQueueFromCreateQueueInput(defaultAuthorization, &sqs.CreateQueueInput{
		QueueName: aws.String(sqslite.DefaultDLQQueueName),
	})
	defaultQueueDLQ.Start(context.Background())
	_ = server.Accounts().EnsureQueues(sqslite.DefaultAccountID).AddQueue(defaultQueueDLQ)
	slog.Info("created default queue dlq with url", slog.String("queue_url", defaultQueueDLQ.URL))

	defaultQueue, _ := sqslite.NewQueueFromCreateQueueInput(defaultAuthorization, &sqs.CreateQueueInput{
		QueueName: aws.String(sqslite.DefaultQueueName),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): marshalJSON(sqslite.RedrivePolicy{
				DeadLetterTargetArn: defaultQueueDLQ.ARN,
				MaxReceiveCount:     3,
			}),
		},
	})
	defaultQueue.Start(context.Background())
	_ = server.Accounts().EnsureQueues(sqslite.DefaultAccountID).AddQueue(defaultQueue)
	slog.Info("created default queue with url", slog.String("queue_url", defaultQueue.URL))

	var handler http.Handler
	if *flagGzip {
		slog.Info("using gzip compression on responses")
		handler = httpz.Logged(httpz.Gzipped(server))
	} else {
		slog.Info("skipping gzip compression on responses")
		handler = httpz.Logged(server)
	}

	httpSrv := &http.Server{
		Addr:    *flagBindAddr,
		Handler: handler,
	}
	group, groupCtx := errgroup.WithContext(context.Background())
	if !*flagSkipStats {
		group.Go(func() error {
			t := time.NewTicker(*flagStatsInterval)
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
	}
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
			oldLevel := logLeveler.Level().String()
			switch logLeveler.Level() {
			case slog.LevelInfo:
				logLeveler.Set(slog.LevelDebug)
			case slog.LevelWarn:
				logLeveler.Set(slog.LevelInfo)
			case slog.LevelError:
				logLeveler.Set(slog.LevelWarn)
			}
			slog.Error("increasing log level", slog.String("old_level", oldLevel), slog.String("new_level", logLeveler.Level().String()))
		}
		decreaseLogLevel := func() {
			oldLevel := logLeveler.Level().String()
			switch logLeveler.Level() {
			case slog.LevelDebug:
				logLeveler.Set(slog.LevelInfo)
			case slog.LevelInfo:
				logLeveler.Set(slog.LevelWarn)
			case slog.LevelWarn:
				logLeveler.Set(slog.LevelError)
			}
			slog.Error("decreasing log level", slog.String("old_level", oldLevel), slog.String("new-level", logLeveler.Level().String()))
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
		doShutdown := func() error {
			shutdownContext, shutdownComplete := context.WithTimeout(context.Background(), *flagShutdownGracePeriod)
			defer shutdownComplete()
			return httpSrv.Shutdown(shutdownContext)
		}
		select {
		case <-groupCtx.Done():
			slog.Error("another process has exited, draining http server gracefully", slog.Duration("grace_period", *flagShutdownGracePeriod))
			return doShutdown()
		case <-ctx.Done():
			slog.Error("shutdown signal received, draining http server gracefully", slog.Duration("grace_period", *flagShutdownGracePeriod))
			return doShutdown()
		}
	})
	if err := group.Wait(); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server exiting with error", slog.Any("err", err))
			os.Exit(1)
		}
	}
}

func printStatistics(server *sqslite.Server, elapsed time.Duration, prev map[string]sqslite.QueueStats) map[string]sqslite.QueueStats {
	var elapsedSeconds float64 = float64(elapsed) / float64(time.Second)
	newStats := make(map[string]sqslite.QueueStats)
	for q := range server.Accounts().EachQueue() {
		prevStats := prev[q.Name]
		stats := q.Stats()
		changeMessagesSent := float64(stats.TotalMessagesSent - prevStats.TotalMessagesSent)
		changeMessagesReceived := float64(stats.TotalMessagesReceived - prevStats.TotalMessagesReceived)
		changeMessagesChangedVisiblity := float64(stats.TotalMessagesChangedVisibility - prevStats.TotalMessagesChangedVisibility)
		changeMessagesDeleted := float64(stats.TotalMessagesDeleted - prevStats.TotalMessagesDeleted)
		changeMessagesPurged := float64(stats.TotalMessagesPurged - prevStats.TotalMessagesPurged)
		changeMessagesDelayedToReady := float64(stats.TotalMessagesDelayedToReady - prevStats.TotalMessagesDelayedToReady)
		changeMessagesInflightToReady := float64(stats.TotalMessagesInflightToReady - prevStats.TotalMessagesInflightToReady)
		changeMessagesInflightToDLQ := float64(stats.TotalMessagesInflightToDLQ - prevStats.TotalMessagesInflightToDLQ)

		var deleted time.Duration
		if !q.Deleted().IsZero() {
			deleted = time.Since(q.Deleted())
		}

		slog.Info(
			"queue statistics",
			slog.String("queue_name", q.Name),
			slog.String("queue_url", q.URL),
			slog.Bool("is_dlq", q.IsDLQ()),
			slog.Duration("created", time.Since(q.Created())),
			slog.Duration("last_modified", time.Since(q.LastModified())),
			slog.Duration("deleted", deleted),
			slog.Int64("num_messages", stats.NumMessages),
			slog.Int64("num_messages_ready", stats.NumMessagesReady),
			slog.Int64("num_messages_inflight", stats.NumMessagesInflight),
			slog.Int64("num_messages_delayed", stats.NumMessagesDelayed),
			slog.String("sent_rate", fmt.Sprintf("%0.2f/sec", changeMessagesSent/elapsedSeconds)),
			slog.String("received_rate", fmt.Sprintf("%0.2f/sec", changeMessagesReceived/elapsedSeconds)),
			slog.String("changed_visibility_rate", fmt.Sprintf("%0.2f/sec", changeMessagesChangedVisiblity/elapsedSeconds)),
			slog.String("deleted_rate", fmt.Sprintf("%0.2f/sec", changeMessagesDeleted/elapsedSeconds)),
			slog.String("purged_rate", fmt.Sprintf("%0.2f/sec", changeMessagesPurged/elapsedSeconds)),
			slog.String("delayed_to_ready_rate", fmt.Sprintf("%0.2f/sec", changeMessagesDelayedToReady/elapsedSeconds)),
			slog.String("inflight_to_ready_rate", fmt.Sprintf("%0.2f/sec", changeMessagesInflightToReady/elapsedSeconds)),
			slog.String("inflight_to_dlq_rate", fmt.Sprintf("%0.2f/sec", changeMessagesInflightToDLQ/elapsedSeconds)),
		)
		newStats[q.Name] = stats
	}
	return newStats
}

func marshalJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}
