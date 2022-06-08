package kafka

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/homework3/moderation/internal/config"
	"github.com/homework3/moderation/internal/tracer"
	"github.com/homework3/moderation/pkg/model"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
)

type producer struct {
	sarama.SyncProducer
	producerTopic string
}

func createProducer(cfg *config.Kafka) (producer producer, err error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Retry.Max = 10
	saramaCfg.Producer.Return.Successes = true

	syncProducer, err := sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return producer, err
	}
	producer.SyncProducer = syncProducer
	producer.producerTopic = cfg.ProducerTopic

	return producer, nil
}

func (p producer) sendComment(span opentracing.Span, comment *model.ModerationComment) error {
	val, err := json.Marshal(comment)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshalize moderation_comment")

		return err
	}

	headers := make([]sarama.RecordHeader, 0, 1)
	tracer.InjectSpanContext(span.Context(), &headers)

	msg := sarama.ProducerMessage{
		Topic:   p.producerTopic,
		Value:   sarama.ByteEncoder(val),
		Headers: headers,
	}

	if _, _, err = p.SendMessage(&msg); err != nil {
		log.Error().Err(err).Msg("Failed to send message into kafkaConsumer")

		return err
	}

	return nil
}
