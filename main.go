package main

import (
	// Register builtin memory and redis engines.
	_ "github.com/nzlov/centrifugo/libcentrifugo/engine/enginemgo"

	// Register embedded web interface.
	_ "github.com/nzlov/centrifugo/libcentrifugo/statik"

	"github.com/nzlov/centrifugo/libcentrifugo/centrifugo"
)

// VERSION of Centrifugo server. Set on build stage.
var VERSION string

func main() {
	centrifugo.Main(VERSION)
}
