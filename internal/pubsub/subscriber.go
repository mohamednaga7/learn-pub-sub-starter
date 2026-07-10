package pubsub

import (
	"bytes"
	"encoding/gob"
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
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var msg T
		err := json.Unmarshal(data, &msg)
		return msg, err
	})
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) shared.AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var msg T
		decoder := gob.NewDecoder(bytes.NewReader(data))
		err := decoder.Decode(&msg)
		return msg, err
	})
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) shared.AckType,
	unmarshaller func([]byte) (T, error),
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
			msg, err := unmarshaller(delivery.Body)
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
