package melta

import (
	"encoding/json"
	"fmt"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/protocol"
)

func sessionInputsFromProtocol(value protocol.SessionInputs) (harness.SessionInputs, error) {
	out := harness.SessionInputs{Instructions: value.Instructions, Prompt: value.Prompt}
	if value.Model != nil {
		ref, err := model.Parse(string(*value.Model))
		if err != nil {
			return out, fmt.Errorf("model: %v: %w", err, harness.ErrInvalid)
		}
		out.Model = &ref
	}
	if value.Tools != nil {
		out.Tools = append([]string{}, (*value.Tools)...)
	}
	if value.Strategy != nil {
		out.Strategy = &harness.StrategySelection{ID: value.Strategy.Id, Settings: json.RawMessage(value.Strategy.Settings)}
	}
	out = out.Owned()
	if _, err := json.Marshal(out); err != nil {
		return harness.SessionInputs{}, fmt.Errorf("inputs: %v: %w", err, harness.ErrInvalid)
	}
	return out, nil
}

func projectSessionInputs(value harness.SessionInputs) protocol.SessionInputs {
	value = value.Owned()
	out := protocol.SessionInputs{Instructions: value.Instructions, Prompt: value.Prompt}
	if value.Model != nil {
		ref := protocol.ModelRef(value.Model.String())
		out.Model = &ref
	}
	if value.Tools != nil {
		out.Tools = &value.Tools
	}
	if value.Strategy != nil {
		out.Strategy = &protocol.StrategySelection{Id: value.Strategy.ID, Settings: string(value.Strategy.Settings)}
	}
	return out
}
