package scenario

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/wcharczuk/sqslite/internal/spy"
)

type ScenarioConfig struct {
	SpyBindAddr  string
	SpyUpstream  string
	BaseEndpoint string
}

type Scenario struct {
	ID     string
	Script func(context.Context, *sqs.Client) error
}

func (s Scenario) Run(ctx context.Context, cfg ScenarioConfig) error {
	output := fmt.Sprintf("testdata/%s.jsonl", s.ID)
	slog.Info("writing output to", slog.String("output", output))
	outputFile, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("unable to create output file: %w", err)
	}
	defer outputFile.Close()

	spyProxy := &http.Server{
		Addr: cfg.SpyBindAddr,
		Handler: &spy.Handler{
			Do:   spy.WriteOutput(io.MultiWriter(outputFile, os.Stdout)),
			Next: httputil.NewSingleHostReverseProxy(must(url.Parse(cfg.SpyUpstream))),
		},
	}
	go func() {
		_ = spyProxy.ListenAndServe()
	}()
	defer func() {
		spyProxy.Shutdown(ctx)
	}()

	sess, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(cfg.BaseEndpoint)
	})
	if s.Script != nil {
		return s.Script(ctx, sqsClient)
	}
	return nil
}

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
