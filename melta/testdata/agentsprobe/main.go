package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/extensions/agents"
)

func main() {
	native := agents.Native(agents.Options{CapabilityIDs: agents.StandardTools, ToolIDs: agents.StandardTools})
	if err := worker.Serve(context.Background(), native, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
