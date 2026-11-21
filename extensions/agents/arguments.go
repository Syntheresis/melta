package agents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// decodeArguments strictly decodes one tool call's JSON arguments into an
// owned map: UseNumber keeps every numeric lexeme exact, and malformed,
// non-object, null or trailing data are argument-validation errors.
func decodeArguments(raw json.RawMessage) (map[string]any, error) {
	var args map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&args); err != nil {
		return nil, fmt.Errorf("arguments must be a JSON object: %w", err)
	}
	if args == nil {
		return nil, errors.New("arguments must be a JSON object")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("arguments must be one JSON object")
	}
	return args, nil
}
