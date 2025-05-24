package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sqslite/pkg/httputil"
	"sqslite/pkg/sqslite"
)

func main() {
	slog.SetDefault(
		slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: false,
		})),
	)
	server := sqslite.NewServer()
	httpSrv := &http.Server{
		Addr:    ":4566",
		Handler: httputil.LoggedHandler(server),
	}
	slog.Info("server listening", slog.String("addr", ":4566"))
	if err := httpSrv.ListenAndServe(); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Info("server exiting with error", slog.Any("err", err))
			os.Exit(1)
		}
	}
}
