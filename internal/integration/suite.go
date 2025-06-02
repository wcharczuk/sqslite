package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"

	sqslite_httputil "github.com/wcharczuk/sqslite/internal/httputil"
	"github.com/wcharczuk/sqslite/internal/spy"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

type Suite struct {
	Region     string
	Mode       Mode
	Clock      clockwork.Clock
	ShowOutput bool
	OutputPath string
}

func (s *Suite) OutputPathOrDefault() string {
	if s.OutputPath != "" {
		return s.OutputPath
	}
	return "testdata/integration"
}

func (s *Suite) RegionOrDefault() string {
	if s.Region != "" {
		return s.Region
	}
	return sqslite.DefaultRegion
}

func (s *Suite) ModeOrDefault() Mode {
	if s.Mode != "" {
		return s.Mode
	}
	return ModeVerify
}

func (s *Suite) ClockOrDefault() clockwork.Clock {
	if s.Clock != nil {
		return s.Clock
	}
	return clockwork.NewRealClock()
}

type Mode string

const (
	ModeUnknown Mode = ""
	ModeSave    Mode = "save"
	ModeVerify  Mode = "verify"
)

func (s *Suite) Run(ctx context.Context, name string, fn func(*Run)) error {
	spyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() {
		_ = spyListener.Close()
	}()
	slog.Debug("starting local spy proxy", slog.String("bind_addr", spyListener.Addr().String()))
	var upstream string
	var sess aws.Config
	var verificationFailures chan *VerificationFailure
	outputPath := filepath.Join(s.OutputPathOrDefault(), fmt.Sprintf("%s.jsonl", name))
	var spyHandler func(spy.Request)
	switch s.ModeOrDefault() {
	case ModeSave:
		slog.Debug("ensuring output path", slog.String("outputPath", outputPath))
		if err := os.MkdirAll(s.OutputPathOrDefault(), 0755); err != nil {
			return fmt.Errorf("unable to create output path dir: %w", err)
		}
		outputFile, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("unable to create output file for write: %w", err)
		}
		defer outputFile.Close()
		if s.ShowOutput {
			spyHandler = spy.WriteOutput(io.MultiWriter(outputFile, os.Stdout))
		} else {
			spyHandler = spy.WriteOutput(io.MultiWriter(outputFile))
		}
		verificationFailures = make(chan *VerificationFailure) // don't do anything with it
		sess, err = config.LoadDefaultConfig(ctx)
		if err != nil {
			return err
		}
		upstream = fmt.Sprintf("https://sqs.%s.amazonaws.com", s.RegionOrDefault())
	case ModeVerify:
		sqsliteListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		defer func() {
			_ = sqsliteListener.Close()
		}()
		slog.Debug("starting local sqslite", slog.String("bind_addr", sqsliteListener.Addr().String()))
		sqsliteServer := &http.Server{
			Handler: sqslite_httputil.Logged(sqslite.NewServer(s.ClockOrDefault())),
		}
		go sqsliteServer.Serve(sqsliteListener)
		defer func() {
			slog.Debug("shutting down local sqslite", slog.String("bind_addr", sqsliteListener.Addr().String()))
			_ = sqsliteServer.Shutdown(context.Background())
		}()
		upstream = fmt.Sprintf("http://%s", sqsliteListener.Addr().String())
		slog.Debug("opening verififer for file", slog.String("outputPath", outputPath))
		v, err := NewVerifier(outputPath)
		if err != nil {
			return err
		}
		defer v.Close()
		spyHandler = v.HandleRequest
		verificationFailures = v.VerificationFailures()
		sess, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(s.Region),
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(sqslite.DefaultAccountID, "test-secret-key", "test-secret-key-token"),
			),
		)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown mode %s", s.ModeOrDefault())
	}

	parsedUpstream, _ := url.Parse(upstream)
	spy := &http.Server{
		Handler: &spy.Handler{
			Do:   spyHandler,
			Next: httputil.NewSingleHostReverseProxy(parsedUpstream),
		},
	}
	go spy.Serve(spyListener)
	defer func() {
		slog.Debug("shutting down local spy proxy", slog.String("bind_addr", spyListener.Addr().String()))
		_ = spy.Shutdown(context.Background())
	}()
	sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", spyListener.Addr().String()))
	})

	suiteExited := make(chan struct{})
	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error {
		select {
		case <-suiteExited:
			return nil
		case <-groupContext.Done():
			return nil
		case failure := <-verificationFailures:
			return failure
		}
	})
	group.Go(func() (err error) {
		it := &Run{
			ctx:       groupContext,
			sqsClient: sqsClient,
			clock:     s.ClockOrDefault(),
		}
		defer func() {
			it.Cleanup()
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
				return
			}
			close(suiteExited)
		}()
		fn(it)
		return
	})
	return group.Wait()
}
