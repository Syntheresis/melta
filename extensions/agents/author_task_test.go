package agents

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

func TestAuthorTaskFrozenStart(t *testing.T) {
	config, err := parse([]byte(`{}`), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	tool := authorTask{definitions: config, settings: defaultSettings()}
	call := model.ToolCall{ID: "call", Arguments: []byte(`{"prompt":"work","subagent_type":"explore"}`)}
	ctx := extension.AuthorContext{Call: func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if service != "melta.session" || method != "members" || string(args) != "{}" {
			t.Fatalf("unexpected host call %s.%s %s", service, method, args)
		}
		return extension.Result{Value: []byte(`[{"kind":"job","id":"j","starter":"agents"}]`)}, nil
	}}
	plan := tool.Prepare(context.Background(), ctx, call)
	if plan.Immediate != nil || plan.Start == nil || plan.Start.Target != "explore" || !reflect.DeepEqual(plan.Start.Inputs, harness.SessionInputs{}) || string(plan.Start.InitialState) != `{"agent":"explore"}` || !plan.Start.Report || plan.Start.OutputLimit != 15360 || !reflect.DeepEqual(plan.Start.Input, []model.ContentPart{{Kind: model.PartText, Text: "work"}}) {
		t.Fatalf("plan = %#v", plan)
	}
	ctx.Call = func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if service != "melta.session" || method != "start" || string(args) != "{}" {
			t.Fatalf("unexpected start %s.%s %s", service, method, args)
		}
		return extension.Result{}, &extension.CallError{Err: errors.New("failed"), Partial: []byte(`{"session_id":"child"}`)}
	}
	out := tool.Execute(context.Background(), ctx, plan)
	if out.Result.Status != model.ResultError || !strings.Contains(out.Result.Content, "child") {
		t.Fatalf("partial = %#v", out)
	}
	ctx.Session = harness.SessionIdentity{OwnerSessionID: "owner"}
	if denied := tool.Prepare(context.Background(), ctx, call); denied.Immediate == nil {
		t.Fatal("owned session accepted task")
	}
	ctx.Session = harness.SessionIdentity{}
	ctx.Call = func(_ context.Context, _, _ string, _ json.RawMessage) (extension.Result, error) {
		return extension.Result{Value: []byte(`[{"kind":"session","id":"one","starter":"agents"}]`)}, nil
	}
	tool.settings.MaxConcurrent = 1
	if denied := tool.Prepare(context.Background(), ctx, call); denied.Immediate == nil {
		t.Fatal("task limit accepted")
	}
}
