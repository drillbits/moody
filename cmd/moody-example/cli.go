package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/drillbits/moody"
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
	ctx := moody.NewContext(context.Background(), cfg)
	moody.Serve(ctx)

	return ExitCodeOK
}
