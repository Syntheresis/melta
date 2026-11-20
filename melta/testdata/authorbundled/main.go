package main

import (
	"context"
	"os"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extension/worker"
	"github.com/Syntheresis/melta/extensions/adaptation"
	"github.com/Syntheresis/melta/extensions/agents"
)

func main() {
	var entry extension.Extension
	if os.Getenv("BUNDLED_EXTENSION") == "adaptation" {
		entry = adaptation.Author()
	} else {
		entry = agents.Author()
	}
	if err := worker.Serve(context.Background(), entry, os.Stdin, os.Stdout); err != nil {
		os.Exit(1)
	}
}
