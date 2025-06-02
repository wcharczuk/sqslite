package integration

import (
	"context"
	"fmt"
	"io"
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

	"github.com/wcharczuk/sqslite/internal/spy"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

type Suite struct {
	Region     string
	Local      bool
	Mode       Mode
	Clock      clockwork.Clock
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

func (s *Suite) Run(ctx context.Context, name string, fn func(*Run)) (err error) {
	var upstream = "https://sqs.us-west-2.amazonaws.com"
	if s.Local {
		upstream = "http://localhost:4566"
	}
	spyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() {
		_ = spyListener.Close()
	}()

	// default ?
	outputPath := filepath.Join(s.OutputPathOrDefault(), fmt.Sprintf("%s.jsonl", name))
	var spyHandler func(spy.Request)
	switch s.ModeOrDefault() {
	case ModeSave:
		if err := os.MkdirAll(s.OutputPathOrDefault(), 0755); err != nil {
			return err
		}
		outputFile, err := os.Create(outputPath)
		if err != nil {
			return err
		}
		defer outputFile.Close()
		spyHandler = spy.WriteOutput(io.MultiWriter(outputFile, os.Stdout))
	case ModeVerify:
		v, err := NewVerifier(outputPath)
		if err != nil {
			return err
		}
		defer v.Close()
		spyHandler = v.HandleRequest
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
		_ = spy.Shutdown(context.Background())
	}()

	var sess aws.Config
	if s.Local {
		sess, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(s.Region),
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(sqslite.DefaultAccountID, "test-secret-key", "test-secret-key-token"),
			),
		)
		if err != nil {
			return err
		}
	} else {
		sess, err = config.LoadDefaultConfig(ctx)
		if err != nil {
			return err
		}
	}
	sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", spyListener.Addr().String()))
	})
	it := &Run{
		ctx:       ctx,
		sqsClient: sqsClient,
		clock:     s.ClockOrDefault(),
	}
	defer func() {
		it.Cleanup()
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
			return
		}
	}()
	fn(it)
	return
}
