package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/shared"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState, _ *amqp.Channel) func(routing.PlayingState) shared.AckType {
	return func(ps routing.PlayingState) shared.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return shared.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, channel *amqp.Channel) func(move gamelogic.ArmyMove) shared.AckType {
	return func(move gamelogic.ArmyMove) shared.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return shared.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(channel, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.GetUsername(), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				return shared.NackRequeue
			}
			return shared.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return shared.NackDiscard
		default:
			return shared.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp.Channel, username, message string) error {
	return pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    username,
	})
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(row gamelogic.RecognitionOfWar) shared.AckType {
	return func(row gamelogic.RecognitionOfWar) shared.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(row)

		var message string
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return shared.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return shared.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			message = fmt.Sprintf("%s won a war against %s", winner, loser)
			break
		case gamelogic.WarOutcomeYouWon:
			message = fmt.Sprintf("%s won a war against %s", winner, loser)
			break
		case gamelogic.WarOutcomeDraw:
			message = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			break
		default:
			log.Println("invalid outcome")
			return shared.NackDiscard
		}

		log.Println(message)
		err := publishGameLog(ch, gs.GetUsername(), message)
		if err != nil {
			return shared.NackRequeue
		}
		return shared.Ack
	}
}
