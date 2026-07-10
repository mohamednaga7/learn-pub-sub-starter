package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Println("Error connecting to rabbitMQ", err)
		return
	}

	defer func(dial *amqp.Connection) {
		_ = dial.Close()
	}(connection)

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Println("Error getting username from user", err)
		return
	}

	gameState := gamelogic.NewGameState(username)

	channel, err := connection.Channel()

	if err != nil {
		log.Println("Error getting the connection channel", err)
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.Transient, handlerPause(gameState, channel))
	if err != nil {
		log.Println("Error subscribing to pause queue for user "+username, err)
		return
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gameState, channel))
	if err != nil {
		log.Println("Error subscribing to the move queue for user "+username, err)
		return
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", pubsub.Durable, handlerWar(gameState, channel))
	if err != nil {
		log.Println("Error subscribing to the move queue for user "+username, err)
		return
	}

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}

		firstWord := inputs[0]

		if firstWord == "spawn" {
			err := gameState.CommandSpawn(inputs)
			if err != nil {
				continue
			}

			fmt.Println("Spawned")

		} else if firstWord == "move" {
			move, err := gameState.CommandMove(inputs)
			if err != nil {
				fmt.Println("Error moving unit", err)
				continue
			}

			fmt.Println("Successfully moved")

			err = pubsub.PublishJSON(channel, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, move)
			if err != nil {
				log.Println("Error publishing the move to the queue", err)
				continue
			}

			fmt.Println("Successfully published the move")
		} else if firstWord == "status" {
			gameState.CommandStatus()

		} else if firstWord == "help" {
			gamelogic.PrintClientHelp()

		} else if firstWord == "spam" {
			if len(inputs) < 2 {
				fmt.Println("usage: spam <number>")
			}

			str := inputs[1]

			number, err := strconv.Atoi(str)
			if err != nil {
				fmt.Println("incorrect number entered")
				continue
			}

			for i := 0; i < number; i++ {
				maliciousLog := gamelogic.GetMaliciousLog()
				err := pubsub.PublishGob(channel, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, routing.GameLog{
					Username:    username,
					CurrentTime: time.Now(),
					Message:     maliciousLog,
				})
				if err != nil {
					log.Println("Error publishing the move to the queue", err)
					continue
				}
			}

			fmt.Println("Spawned")

		} else if firstWord == "quit" {
			gamelogic.PrintQuit()
			break

		} else {
			fmt.Println("Incorrect input")
		}

	}

	fmt.Println("Client is shutting down...")
}
