package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	dial, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println("Error connecting to rabbitMQ", err)
		return
	}

	defer func(dial *amqp.Connection) {
		_ = dial.Close()
	}(dial)

	channel, err := dial.Channel()
	if err != nil {
		log.Println("Error creating a rabbitMQ channel", err)
		return
	}

	closable, _, err := pubsub.DeclareAndBind(dial, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable)
	if err != nil {
		return
	}

	defer func(closable *amqp.Channel) {
		_ = closable.Close()
	}(closable)

	fmt.Println("Started listening...")

	gamelogic.PrintServerHelp()
	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}

		firstWord := inputs[0]

		if firstWord == "pause" {
			log.Println("Sending pause message")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Println("Error sending message", err)
				continue
			}
		} else if firstWord == "resume" {
			log.Println("Sending resume message")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Println("Error sending message", err)
				continue
			}
		} else if firstWord == "quit" {
			log.Println("Quitting the app")
			break
		} else {
			log.Println("Couldn't understand the command")
		}

	}

	fmt.Println("Server is shutting down...")
}
