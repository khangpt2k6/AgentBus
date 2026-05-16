package main

import (
	"fmt"
	"os"

	"github.com/khangpt2k6/AgentBus/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
