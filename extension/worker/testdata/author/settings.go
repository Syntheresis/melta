package main

import (
	"context"
	"encoding/json"

	"github.com/Syntheresis/melta/extension"
)

func settingsFixture() extension.Extension {
	return extension.Extension{
		Manifest: extension.Manifest{ID: "settings-worker", Version: "1", APIVersion: extension.APIVersion, SettingsSchema: []byte(`{"type":"object"}`), Services: []string{"melta.settings", "melta.warnings"}},
		Activate: func(ctx context.Context, api extension.API) (func(context.Context) error, error) {
			if _, err := api.Call(ctx, "melta.warnings", "report", []byte(`{"kind":"worker-entry","message":"before"}`)); err != nil {
				return nil, err
			}
			_, err := api.RegisterService(extension.Service{Name: "worker/transform", Methods: []extension.RegisteredMethod{{
				Method: extension.Method{Name: "change/value", Mode: extension.ModePure, Input: []byte(`{}`), Success: []byte(`{}`)},
				Handler: func(_ context.Context, _ extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					var input struct {
						Section   map[string]json.RawMessage `json:"section"`
						Arguments json.RawMessage            `json:"arguments"`
					}
					if err := json.Unmarshal(args, &input); err != nil {
						return extension.Result{}, err
					}
					input.Section["value"] = input.Arguments
					result, err := json.Marshal(input.Section)
					return extension.Result{Value: result}, err
				},
			}, {
				Method: extension.Method{Name: "warn", Mode: extension.ModeServiceIO, Input: []byte(`{}`), Success: []byte(`{}`)},
				Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
					return call.Call(ctx, "melta.warnings", "report", args)
				},
			}}}, nil)
			if err == nil {
				_, err = api.RegisterOperation(extension.Operation{
					Target: extension.ExposedTargetHarness,
					Method: extension.Method{Name: "update", Mode: extension.ModeServiceIO, Input: []byte(`{}`), Success: []byte(`{}`)},
					Handler: func(ctx context.Context, call extension.AuthorContext, args json.RawMessage) (extension.Result, error) {
						return call.Call(ctx, "melta.settings", "update", args)
					},
				}, nil)
			}
			return nil, err
		},
	}
}
