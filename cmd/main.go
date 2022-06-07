package main

import (
	"github.com/homework3/moderation/internal/app"
	"github.com/homework3/moderation/internal/config"
	"github.com/homework3/moderation/internal/tracer"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	if err := config.ReadConfigYML("config.yml"); err != nil {
		log.Fatal().Err(err).Msg("Failed to read configuration")
	}

	cfg := config.GetConfigInstance()

	if cfg.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	tracing, err := tracer.NewTracer(&cfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed init tracing")

		return
	}
	defer tracing.Close()

	if err = app.New().Start(&cfg); err != nil {
		log.Error().Err(err).Msg("Failed to start app")
	}
}
