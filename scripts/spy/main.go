package main

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	"github.com/spf13/pflag"
	"github.com/wcharczuk/sqslite/internal/spy"
)

var (
	flagBindAddr = pflag.String("bind-addr", ":4567", "The bind address")
	flagUpstream = pflag.String("upstream", "https://sqs.us-west-2.amazonaws.com", "The upstream address (leave blank to use the default)")
)

func main() {
	pflag.Parse()
	s := &http.Server{
		Addr: *flagBindAddr,
		Handler: &spy.Handler{
			Out:  os.Stdout,
			Next: httputil.NewSingleHostReverseProxy(must(url.Parse(*flagUpstream))),
		},
	}
	slog.Info("listening on bind address", slog.String("bind_addr", *flagBindAddr))
	if err := s.ListenAndServe(); err != nil {
		slog.Error("server exited", slog.Any("err", err))
		os.Exit(1)
	}
}

func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
