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
	config, err := parse([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	tool := authorTask{definitions: config, settings: defaultSettings()}
	call := extension.ToolCall{ID: "call", Arguments: []byte(`{"prompt":"work","subagent_type":"explore"}`)}
	ctx := extension.AuthorContext{Call: func(_ context.Context, service, method string, args json.RawMessage) (extension.Result, error) {
		if service != "melta.session" {
			t.Fatalf("unexpected host call %s.%s %s", service, method, args)
		}
		if method == "members" {
			return extension.Result{Value: []byte(`[{"kind":"job","id":"j","starter":"agents"}]`)}, nil
		}
		start, err := extension.DecodeSessionStart(args)
		if method != "start" || err != nil || !reflect.DeepEqual(start.Inputs, harness.SessionInputs{}) || string(start.InitialState) != `{"agent":"explore"}` || !start.Report || start.OutputLimit != 15360 || !reflect.DeepEqual(start.Input, []model.ContentPart{{Kind: model.PartText, Text: "work"}}) {
			t.Fatalf("start = %#v, %v", start, err)
		}
		return extension.Result{}, &extension.CallError{Err: errors.New("failed"), Partial: []byte(`{"session_id":"child"}`)}
	}}
	call.AuthorContext = ctx
	_, err = tool.run(context.Background(), call)
	if err == nil || !strings.Contains(err.Error(), "child") {
		t.Fatalf("partial = %v", err)
	}
	call.Session = harness.SessionIdentity{OwnerSessionID: "owner"}
	if _, denied := tool.run(context.Background(), call); denied == nil {
		t.Fatal("owned session accepted task")
	}
	call.Session = harness.SessionIdentity{}
	call.Call = func(_ context.Context, _, _ string, _ json.RawMessage) (extension.Result, error) {
		return extension.Result{Value: []byte(`[{"kind":"session","id":"one","starter":"agents"}]`)}, nil
	}
	tool.settings.MaxConcurrent = 1
	if _, denied := tool.run(context.Background(), call); denied == nil {
		t.Fatal("task limit accepted")
	}
}
