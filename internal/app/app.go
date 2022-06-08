package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/homework3/moderation/internal/config"
	"github.com/homework3/moderation/internal/kafka"
	"github.com/homework3/moderation/internal/metrics"
	"github.com/rs/zerolog/log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

type App struct {
}

func New() *App {
	return &App{}
}

func (a *App) Start(cfg *config.Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metricsAddr := fmt.Sprintf("%s:%v", cfg.Metrics.Host, cfg.Metrics.Port)

	metricsServer := metrics.CreateMetricsServer(metricsAddr, cfg)

	go func() {
		log.Info().Msgf("Metrics http_server is running on %s", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("Failed running metrics http_server")
			cancel()
		}
	}()

	go func() {
		if err := kafka.StartObserveMessages(ctx, &cfg.Kafka); err != nil {
			log.Error().Err(err).Msg("Failed to start kafka client")
			cancel()
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	select {
	case v := <-quit:
		log.Info().Msgf("signal.Notify: %v", v)
	case done := <-ctx.Done():
		log.Info().Msgf("ctx.Done: %v", done)
	}

	if err := metricsServer.Shutdown(ctx); err != nil {
		log.Error().Err(err).Msg("metricsServer.Shutdown")
	} else {
		log.Info().Msg("metricsServer shut down correctly")
	}

	return nil
}
