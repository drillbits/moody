package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/drillbits/moody"
	"google.golang.org/api/iterator"
)

// Exit codes are int values that represent an exit code for a particular error.
const (
	ExitCodeOK    int = 0
	ExitCodeError int = 1 + iota
)

// CLI is the command line object
type CLI struct {
	// outStream and errStream are the stdout and stderr
	// to write message from the CLI.
	outStream, errStream io.Writer
}

const subName = "/cloud-app"

// Run invokes the CLI with the given arguments.
func (cli *CLI) Run(args []string) int {
	var (
		config string

		version bool
	)

	// Define option flag parse
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(cli.errStream)

	flags.StringVar(&config, "config", "", "Location of config file")
	flags.StringVar(&config, "c", "", "Location of config file(Short)")

	flags.BoolVar(&version, "version", false, "Print version information and quit.")

	// Parse commandline flag
	if err := flags.Parse(args[1:]); err != nil {
		return ExitCodeError
	}

	// Show version
	if version {
		fmt.Fprintf(cli.errStream, "%s version %s\n", name, ver)
		return ExitCodeOK
	}

	cfg, err := moody.NewConfig(config)
	if err != nil {
		fmt.Fprintf(cli.errStream, "Error: %s\n", err)
		return ExitCodeError
	}
	ctx := moody.NewContext(context.Background(), cfg)

	c, err := moody.NewCloudPubSubClient(ctx)
	if err != nil {
		fmt.Fprintf(cli.errStream, "Error: %s\n", err)
		return ExitCodeError
	}

	for _, rawid := range moody.TestTopics {
		topic, err := moody.CreateTopicIfNotExists(ctx, c, rawid)
		if err != nil {
			fmt.Fprintf(cli.errStream, "Error: %s\n", err)
			return ExitCodeError
		}
		_, err = moody.CreateSubscriptionIfNotExists(ctx, c, rawid+subName, topic, 10*time.Second, nil)
		if err != nil {
			fmt.Fprintf(cli.errStream, "Error: %s\n", err)
			return ExitCodeError
		}
	}

	// publish
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "favicon.ico") {
			return
		}
		rawid := r.URL.Path[1:]
		id := url.QueryEscape(rawid)
		data := []byte(fmt.Sprint(time.Now()))

		topic := c.Topic(id)
		_, err := topic.Publish(ctx, &pubsub.Message{
			Data: data,
		})
		if err != nil {
			w.Write([]byte(err.Error()))
		} else {
			log.Printf("Publish topic: %s, data: %s", id, string(data))
			w.Write([]byte(rawid + " : "))
			w.Write(data)
		}
	})

	// subscribe
	for _, rawid := range moody.TestTopics {
		id := url.QueryEscape(rawid + subName)
		sub := c.Subscription(id)
		go func(ctx context.Context, sub *pubsub.Subscription) {
			it, err := sub.Pull(ctx)
			if err != nil {
				log.Printf("Error: %s\n", err)
				return
			}
			defer it.Stop()

			log.Printf("Subscribe: %s", sub.ID())
			for {
				m, err := it.Next()
				if err == iterator.Done {
					break
				} else if err != nil {
					log.Printf("Error: %s\n", err)
					break
				}
				msg, err := moody.NewMessage(m.Data)
				if err != nil {
					// TODO: Normal Pub/Sub
					// log.Printf("Error: %s, %s\n", err, string(m.Data))
				} else {
					log.Printf("Receive topic: %s, msg: %s", sub.ID(), string(msg.Data))
				}
				m.Done(true)
			}
			log.Printf("Done: %s", sub.ID())
		}(ctx, sub)
	}

	err = http.ListenAndServe(":8002", nil)
	if err != nil {
		fmt.Fprintf(cli.errStream, "Error: %s\n", err)
		return ExitCodeError
	}

	return ExitCodeOK
}
