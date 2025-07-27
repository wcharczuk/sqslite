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
	"golang.org/x/sync/errgroup"

	"github.com/wcharczuk/sqslite/internal/spy"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

type Suite struct {
	Local  bool
	Region string

	SkipShowOutput  bool
	SkipWriteOutput bool

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

func (s *Suite) ShouldUseSpyProxy() bool {
	return !s.SkipShowOutput || !s.SkipWriteOutput
}

func (s *Suite) Run(ctx context.Context, id string, fn func(*Run)) error {
	var err error
	var upstream string
	var sess aws.Config
	if s.Local {
		sess, err = config.LoadDefaultConfig(ctx,
			config.WithRegion("us-west-2"),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(sqslite.DefaultAccountID, "test-secret-key", "test-secret-key-token")),
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

	var sqsClient *sqs.Client
	if s.ShouldUseSpyProxy() {
		spyListener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		defer spyListener.Close()
		var spyHandler func(spy.Request)
		if !s.SkipWriteOutput {
			var outputPath string
			if s.Local {
				outputPath = filepath.Join(s.OutputPathOrDefault(), fmt.Sprintf("%s.local.jsonl", id))
			} else {
				outputPath = filepath.Join(s.OutputPathOrDefault(), fmt.Sprintf("%s.jsonl", id))
			}
			if err := os.MkdirAll(s.OutputPathOrDefault(), 0755); err != nil {
				return fmt.Errorf("unable to create output path dir: %w", err)
			}
			outputFile, err := os.Create(outputPath)
			if err != nil {
				return fmt.Errorf("unable to create output file for write: %w", err)
			}
			defer outputFile.Close()
			if s.SkipShowOutput {
				spyHandler = WriteOutput(io.MultiWriter(outputFile))
			} else {
				spyHandler = WriteOutput(io.MultiWriter(outputFile, os.Stdout))
			}
		} else {
			if !s.SkipShowOutput {
				spyHandler = WriteOutput(os.Stdout)
			} else {
				spyHandler = func(spy.Request) {}
			}
		}
		if s.Local {
			upstream = "http://localhost:4566"
		} else {
			upstream = fmt.Sprintf("https://sqs.%s.amazonaws.com", s.RegionOrDefault())
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
		sqsClient = sqs.NewFromConfig(sess, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", spyListener.Addr().String()))
		})
	} else {
		if s.Local {
			sqsClient = sqs.NewFromConfig(sess, func(o *sqs.Options) {
				o.BaseEndpoint = aws.String("http://localhost:4566")
			})
		} else {
			sqsClient = sqs.NewFromConfig(sess)
		}
	}
	group, groupContext := errgroup.WithContext(ctx)
	it := &Run{
		id:        id,
		ctx:       groupContext,
		sqsClient: sqsClient,
	}
	suiteExited := make(chan struct{})
	group.Go(func() (err error) {
		for {
			select {
			case <-suiteExited:
				return
			case <-groupContext.Done():
				return
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
