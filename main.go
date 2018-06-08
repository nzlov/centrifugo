package main

import (
	// Register builtin memory and redis engines.
	"log"

	"github.com/google/gops/agent"
	_ "github.com/nzlov/centrifugo/libcentrifugo/engine/enginegorm"

	// Register embedded web interface.
	_ "github.com/nzlov/centrifugo/libcentrifugo/statik"

	"github.com/nzlov/centrifugo/libcentrifugo/centrifugo"
)

// VERSION of Centrifugo server. Set on build stage.
var VERSION string

func main() {
	if err := agent.Listen(agent.Options{
		Addr: ":8010",
	}); err != nil {
		log.Fatal(err)
	}
	centrifugo.Main(VERSION)
}
