package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/jonboulle/clockwork"
	"github.com/spf13/pflag"
	"github.com/wcharczuk/sqslite/internal/sqslite/integration"
)

var (
	flagSpyBindAddr       = pflag.String("spy-bind-addr", ":4567", "The bind address of the spy proxy")
	flagSpyUpstream       = pflag.String("spy-upstream", "https://sqs.us-west-2.amazonaws.com", "The upstream url the spy proxy will forward to")
	flagEnabledScenarios  = pflag.StringArray("enable-scenario", []string{"visibility_timeouts"}, "The scenarios that are explicitly enabled")
	flagDisabledScenarios = pflag.StringArray("disable-scenario", nil, "The scenarios that are explicitly disabled")
)

var scenarios = []integration.Scenario{
	integration.Basic,
	integration.VisibilityTimeouts,
}

func main() {
	pflag.Parse()
	slog.SetDefault(
		slog.New(
			slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{}),
		),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	sess, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	cfg := integration.ScenarioConfig{
		SpyBindAddr: *flagSpyBindAddr,
		SpyUpstream: *flagSpyUpstream,
		AWSConfig:   sess,
		Clock:       clockwork.NewRealClock(),
		Mode:        integration.ModeSave,
	}

	enabled := make(map[string]struct{})
	for _, scenarioID := range *flagEnabledScenarios {
		enabled[scenarioID] = struct{}{}
	}
	disabled := make(map[string]struct{})
	for _, scenarioID := range *flagDisabledScenarios {
		disabled[scenarioID] = struct{}{}
	}
	for _, scenario := range scenarios {
		if _, ok := enabled[scenario.ID]; len(enabled) > 0 && !ok {
			continue
		}
		if _, ok := disabled[scenario.ID]; len(disabled) > 0 && ok {
			continue
		}
		slog.Info("scenario starting", slog.String("scenario", scenario.ID))
		if err := scenario.Run(ctx, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		slog.Info("scenario complete", slog.String("scenario", scenario.ID))
	}
}
