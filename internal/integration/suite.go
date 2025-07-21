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
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"

	"github.com/wcharczuk/sqslite/internal/httpz"
	"github.com/wcharczuk/sqslite/internal/spy"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

type Suite struct {
	Local      bool
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

func (s *Suite) Run(ctx context.Context, id string, fn func(*Run)) error {
	spyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer spyListener.Close()

	var upstream string
	var sess aws.Config
	var verificationFailures chan *VerificationFailure

	var outputPath string
	if s.Local {
		outputPath = filepath.Join(s.OutputPathOrDefault(), fmt.Sprintf("%s.local.jsonl", id))
	} else {
		outputPath = filepath.Join(s.OutputPathOrDefault(), fmt.Sprintf("%s.jsonl", id))
	}
	var spyHandler func(spy.Request)
	switch s.ModeOrDefault() {
	case ModeSave:
		if err := os.MkdirAll(s.OutputPathOrDefault(), 0755); err != nil {
			return fmt.Errorf("unable to create output path dir: %w", err)
		}
		outputFile, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("unable to create output file for write: %w", err)
		}
		defer outputFile.Close()
		if s.ShowOutput {
			spyHandler = WriteAndNormalizeOutput(io.MultiWriter(outputFile, os.Stdout))
		} else {
			spyHandler = WriteAndNormalizeOutput(io.MultiWriter(outputFile))
		}
		verificationFailures = make(chan *VerificationFailure) // don't do anything with it
		sess, err = config.LoadDefaultConfig(ctx)
		if err != nil {
			return err
		}
		if s.Local {
			upstream = "http://localhost:4566"
		} else {
			upstream = fmt.Sprintf("https://sqs.%s.amazonaws.com", s.RegionOrDefault())
		}
	case ModeVerify:
		sess, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(s.Region),
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(sqslite.DefaultAccountID, "test-secret-key", "test-secret-key-token"),
			),
		)
		if err != nil {
			return err
		}

		sqsliteListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		defer sqsliteListener.Close()

		sqsliteServer := &http.Server{
			Handler: httpz.Logged(sqslite.NewServer(s.ClockOrDefault())),
		}

		go sqsliteServer.Serve(sqsliteListener)
		defer sqsliteServer.Shutdown(context.Background())

		v, err := NewVerifier(outputPath)
		if err != nil {
			return err
		}
		defer v.Close()
		spyHandler = v.HandleRequest
		verificationFailures = v.VerificationFailures()
		upstream = fmt.Sprintf("http://%s", sqsliteListener.Addr().String())
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
	defer spy.Shutdown(context.Background())

	sqsClient := sqs.NewFromConfig(sess, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", spyListener.Addr().String()))
	})
	group, groupContext := errgroup.WithContext(ctx)
	it := &Run{
		id:        id,
		ctx:       groupContext,
		sqsClient: sqsClient,
		clock:     s.ClockOrDefault(),
	}
	suiteExited := make(chan struct{})
	group.Go(func() (err error) {
		var errOnce sync.Once
		for {
			select {
			case <-suiteExited:
				return
			case <-groupContext.Done():
				return
			case failure := <-verificationFailures:
				errOnce.Do(func() {
					err = failure
				})
			}
		}
	})

	group.Go(func() (err error) {
		defer func() {
			defer close(suiteExited)
			it.Cleanup()
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
				return
			}
		}()
		fn(it)
		return
	})
	return group.Wait()
}
