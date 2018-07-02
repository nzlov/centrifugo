package enginegorm

import (
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/server"

	cli "github.com/micro/go-plugins/client/grpc"
	srv "github.com/micro/go-plugins/server/grpc"
)

func init() {
	// set the default client
	client.DefaultClient = cli.NewClient()
	// set the default server
	server.DefaultServer = srv.NewServer()
}
