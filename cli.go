package main

import (
	"github.com/kimuraz/golang-json-db/client"
	"github.com/kimuraz/golang-json-db/server"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"os"
)

func startServer(cCtx *cli.Context) error {
	config := NewConfig("config.json")
	server := server.NewServer(config.ServerPort)

	go server.StartServer()

	log.Info().Msg("Server started")

	for {
		message := <-server.MessageChan
		if message != "" {
			log.Info().Msg(message)
		}
	}

	return nil
}

func connClient(cCtx *cli.Context) error {
	port := cCtx.Args().First()
	cl := client.NewClient()
	cl.Connect(port)

	return nil
}

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	commands := []*cli.Command{
		{
			Name:    "server",
			Aliases: []string{"svr"},
			Usage:   "Server commands, it uses config.json by default",
			Subcommands: []*cli.Command{
				{
					Name:     "start",
					Category: "server",
					Usage:    "Starts the server",
					Action:   startServer,
				},
			},
		},
		{
			Name:    "client",
			Aliases: []string{"cl"},
			Usage:   "Client commands",
			Subcommands: []*cli.Command{
				{
					Name:     "connect",
					Category: "client",
					Usage:    "Connects to a server",
					Action:   connClient,
				},
			},
		},
	}

	app := &cli.App{
		Name:     "gjdb",
		Usage:    "Golang JSON DB is a fun simple project implementing a JSON-based db from scratch",
		Commands: commands,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal().Err(err)
		os.Exit(1)
	}
}
