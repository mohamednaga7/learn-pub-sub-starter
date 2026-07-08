package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/shared"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) shared.AckType {
	return func(ps routing.PlayingState) shared.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return shared.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(move gamelogic.ArmyMove) shared.AckType {
	return func(move gamelogic.ArmyMove) shared.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return shared.Ack
		case gamelogic.MoveOutcomeMakeWar:
			return shared.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return shared.NackDiscard
		default:
			return shared.NackDiscard
		}
	}
}
