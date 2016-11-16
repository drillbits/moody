package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/drillbits/moody"
	"github.com/garyburd/redigo/redis"
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

	c, err := redis.DialURL(cfg.RedisURI)
	if err != nil {
		fmt.Fprintf(cli.errStream, "Error: %s\n", err)
		return ExitCodeError
	}

	// publish
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "favicon.ico") {
			return
		}
		rawid := r.URL.Path[1:]
		data := []byte(fmt.Sprint(time.Now()))

		_, err := c.Do("PUBLISH", rawid, data)
		if err != nil {
			w.Write([]byte(err.Error()))
		} else {
			log.Printf("Publish topic: %s, data: %s", rawid, string(data))
			w.Write([]byte(rawid + " : "))
			w.Write(data)
		}
	})

	// subscribe
	c2, err := redis.DialURL(cfg.RedisURI)
	if err != nil {
		fmt.Fprintf(cli.errStream, "Error: %s\n", err)
		return ExitCodeError
	}
	psc := redis.PubSubConn{Conn: c2}
	for _, rawid := range cfg.Topics {
		err := psc.Subscribe(rawid)
		if err != nil {
			fmt.Fprintf(cli.errStream, "Error: %s\n", err)
			return ExitCodeError
		}
		log.Printf("Subscribe: %s", rawid)
	}
	go func() {
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				msg, err := moody.NewMessage(v.Data)
				if err != nil {
					// TODO: Normal Pub/Sub
					// log.Printf("Error: %s, %s\n", err, string(v.Data))
				} else {
					log.Printf("Receive topic: %s, data: %s", v.Channel, string(msg.Data))
				}
				// TODO: ack done?
			case error:
				log.Println(v)
			}
		}
	}()

	err = http.ListenAndServe(":8001", nil)
	if err != nil {
		fmt.Fprintf(cli.errStream, "Error: %s\n", err)
		return ExitCodeError
	}

	return ExitCodeOK
}
