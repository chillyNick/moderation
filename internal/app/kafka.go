package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/homework3/moderation/internal/config"
	"github.com/homework3/moderation/internal/tracer"
	"github.com/homework3/moderation/pkg/model"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
)

type kafka struct {
	producer      sarama.SyncProducer
	producerTopic string
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (k *kafka) Setup(session sarama.ConsumerGroupSession) error {
	log.Info().Msg("Setup consumer group session")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (k *kafka) Cleanup(sarama.ConsumerGroupSession) error {
	log.Info().Msg("cleanup")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (k *kafka) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Info().Msg(fmt.Sprintf("Start consumer loop for topic: %s", claim.Topic()))
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// <https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29>
	for msg := range claim.Messages() {
		log.Info().
			Str("value", string(msg.Value)).
			Msgf("Message topic:%q partition:%d offset:%d", msg.Topic, msg.Partition, msg.Offset)

		spanCtx, err := tracer.ExtractSpanContext(msg.Headers)
		if err != nil {
			log.Error().Err(err).Msg("Failed to extract spanContext from kafka headers")
		}
		span := opentracing.StartSpan("Comment moderation", opentracing.ChildOf(spanCtx))

		comment := model.Comment{}
		err = json.Unmarshal(msg.Value, &comment)
		if err != nil {
			log.Error().Err(err).Str("value", string(msg.Value)).Msg("Failed to unmarshal comment")
			span.Finish()

			continue
		}

		err = k.processComment(comment)
		span.Finish()
		if err != nil {
			log.Error().Err(err).Msg("Failed to process message")

			continue
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

func (k *kafka) processComment(comment model.Comment) error {
	mComment := ModerateComment(comment)

	val, err := json.Marshal(mComment)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshalize moderation_comment")

		return err
	}

	msg := sarama.ProducerMessage{
		Topic: k.producerTopic,
		Value: sarama.ByteEncoder(val),
	}

	if _, _, err = k.producer.SendMessage(&msg); err != nil {
		log.Error().Err(err).Msg("Failed to send message into kafka")

		return err
	}

	return nil
}

func observeMbMessages(ctx context.Context, cfg *config.Kafka) error {
	consumer, err := createConsumerGroup(cfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create a consumer group")

		return err
	}
	defer consumer.Close()

	producer, err := createProducer(cfg.Brokers)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create a producer")

		return err
	}
	defer producer.Close()

	handler := &kafka{producer: producer, producerTopic: cfg.ProducerTopic}
	loop := true
	for loop {
		err = consumer.Consume(ctx, []string{cfg.ConsumerTopic}, handler)
		if err != nil {
			log.Error().Err(err).Msg(" Consumer group session error")
		}

		select {
		case <-ctx.Done():
			loop = false
		default:

		}
	}

	return nil
}

func createConsumerGroup(cfg *config.Kafka) (sarama.ConsumerGroup, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	return sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupId, saramaCfg)
}

func createProducer(brokerList []string) (sarama.SyncProducer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Retry.Max = 10
	saramaCfg.Producer.Return.Successes = true

	return sarama.NewSyncProducer(brokerList, saramaCfg)
}
