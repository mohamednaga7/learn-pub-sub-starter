package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/shared"
	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) shared.AckType,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	subscriberChannel, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func(ch <-chan amqp.Delivery) {
		defer func(channel *amqp.Channel) {
			_ = channel.Close()
		}(channel)

		for delivery := range ch {
			var msg T
			err := json.Unmarshal(delivery.Body, &msg)
			if err != nil {
				fmt.Printf("error unmarshaling message: %v\n", err)
				continue
			}
			ack := handler(msg)
			if ack == shared.Ack {
				log.Println("Acknowledging the message")
				err := delivery.Ack(false)
				if err != nil {
					log.Println("Error acknowledging the message")
				}
			} else if ack == shared.NackRequeue {
				log.Println("Adding the message to the requeue for reacknowledgement")
				err := delivery.Nack(false, true)
				if err != nil {
					log.Println("Error Not acknowledging the message and adding it to the requeue")
				}
			} else if ack == shared.NackDiscard {
				log.Println("Discarding the message")
				err := delivery.Nack(false, false)
				if err != nil {
					log.Println("Error Not acknowledging the message and Discarding it")
				}
			}
		}
	}(subscriberChannel)

	_ = queue
	return nil
}
