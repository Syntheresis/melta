package extension

import (
	"bytes"
	"context"
	"encoding"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
)

type ToolFunc func(context.Context, ToolCall) (ToolResult, error)

type ToolCall struct {
	AuthorContext
	ID        string
	Name      string
	Arguments json.RawMessage
}

type ToolResult struct {
	Content  string
	Metadata json.RawMessage
}

var ErrOutcomeUnknown = errors.New("extension: outcome unknown")

// Decode decodes the tool arguments into v. Member names match exactly at every
// level: a member that differs only in case from a struct field is ignored, and
// a repeated name keeps only its last value.
func (c ToolCall) Decode(v any) error {
	raw := bytes.TrimSpace(c.Arguments)
	if len(raw) == 0 || raw[0] != '{' {
		return errors.New("tool arguments must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(exactMemberNames(raw, reflect.TypeOf(v))))
	decoder.UseNumber()
	if err := decoder.Decode(v); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return errors.New("tool arguments contain trailing JSON")
		}
		return err
	}
	return nil
}

var (
	jsonUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

// decodesItself reports whether t decodes its own JSON, by value or pointer
// receiver. Such a type owns its member rules and is never rewritten.
func decodesItself(t reflect.Type) bool {
	return t.Implements(jsonUnmarshalerType) || t.Implements(textUnmarshalerType) ||
		reflect.PointerTo(t).Implements(jsonUnmarshalerType) || reflect.PointerTo(t).Implements(textUnmarshalerType)
}

// opaqueType reports whether t has no struct member names to match.
func opaqueType(t reflect.Type) bool {
	return t == nil || t.Kind() == reflect.Interface || decodesItself(t)
}

// exactMemberNames rebuilds one raw JSON value guided by its target type so
// that every object decoded into a struct keeps only the members named exactly
// after its fields. Member values keep their bytes; a value of a different JSON
// kind is left to the unchanged decode's error, and a type that decodes itself,
// an interface or a scalar is not rewritten.
func exactMemberNames(raw []byte, t reflect.Type) []byte {
	if opaqueType(t) {
		return raw
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
		if opaqueType(t) {
			return raw
		}
	}
	switch t.Kind() {
	case reflect.Struct:
		return exactStructMemberNames(raw, t)
	case reflect.Slice, reflect.Array:
		if opaqueType(t.Elem()) {
			return raw
		}
		return exactElementMemberNames(raw, t.Elem())
	case reflect.Map:
		if opaqueType(t.Elem()) {
			return raw
		}
		return exactMapValueMemberNames(raw, t.Elem())
	}
	return raw
}

// exactStructMemberNames keeps only the members named exactly after a struct
// field; the decoded map already keeps only a repeated name's last value. A
// value that is not a JSON object is left to the decoder.
func exactStructMemberNames(raw []byte, t reflect.Type) []byte {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil || members == nil {
		return raw
	}
	fields := jsonFieldTypes(t)
	for name, value := range members {
		field, ok := fields[name]
		if !ok {
			delete(members, name)
			continue
		}
		members[name] = exactMemberNames(value, field)
	}
	return writeObject(members)
}

// exactElementMemberNames rewrites each element of one array value.
func exactElementMemberNames(raw []byte, elem reflect.Type) []byte {
	var elements []json.RawMessage
	if err := json.Unmarshal(raw, &elements); err != nil || elements == nil {
		return raw
	}
	for i, element := range elements {
		elements[i] = exactMemberNames(element, elem)
	}
	return writeArray(elements)
}

// exactMapValueMemberNames rewrites the values of one object value; keys are
// untouched and a repeated key already keeps its last value.
func exactMapValueMemberNames(raw []byte, elem reflect.Type) []byte {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil || members == nil {
		return raw
	}
	for name, value := range members {
		members[name] = exactMemberNames(value, elem)
	}
	return writeObject(members)
}

// writeObject writes members as one JSON object in name order, each value as
// its own encoded bytes; json.Marshal would compact and re-escape them.
func writeObject(members map[string]json.RawMessage) []byte {
	names := make([]string, 0, len(members))
	for name := range members {
		names = append(names, name)
	}
	sort.Strings(names)
	var out bytes.Buffer
	out.WriteByte('{')
	for i, name := range names {
		if i > 0 {
			out.WriteByte(',')
		}
		key, _ := json.Marshal(name)
		out.Write(key)
		out.WriteByte(':')
		out.Write(members[name])
	}
	out.WriteByte('}')
	return out.Bytes()
}

// writeArray writes elements back as one JSON array.
func writeArray(elements []json.RawMessage) []byte {
	var out bytes.Buffer
	out.WriteByte('[')
	for i, element := range elements {
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(element)
	}
	out.WriteByte(']')
	return out.Bytes()
}

// jsonField is one struct field name and the type decoded into it.
type jsonField struct {
	typ   reflect.Type
	depth int
}

// jsonFieldTypes maps each JSON name encoding/json reads for t to its field
// type: the tag name before the first comma or else the Go name of an exported
// field; "json:\"-\"" and unexported non-embedded fields are skipped; fields of
// an untagged embedded struct or pointer to struct are promoted.
func jsonFieldTypes(t reflect.Type) map[string]reflect.Type {
	found := make(map[string]jsonField)
	addJSONFields(t, 0, make(map[reflect.Type]bool), found)
	fields := make(map[string]reflect.Type, len(found))
	for name, field := range found {
		fields[name] = field.typ
	}
	return fields
}

func addJSONFields(t reflect.Type, depth int, visiting map[reflect.Type]bool, found map[string]jsonField) {
	if visiting[t] {
		return
	}
	visiting[t] = true
	defer delete(visiting, t)
	var embedded []reflect.Type
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		embeddedType := field.Type
		if embeddedType.Kind() == reflect.Ptr {
			embeddedType = embeddedType.Elem()
		}
		if field.Anonymous && name == "" && embeddedType.Kind() == reflect.Struct {
			embedded = append(embedded, embeddedType)
			continue
		}
		if !field.IsExported() {
			continue
		}
		if name == "" {
			name = field.Name
		}
		if current, ok := found[name]; ok && current.depth <= depth {
			continue
		}
		found[name] = jsonField{typ: field.Type, depth: depth}
	}
	for _, embeddedType := range embedded {
		addJSONFields(embeddedType, depth+1, visiting, found)
	}
}

func RunTool(ctx context.Context, f ToolFunc, call ToolCall) harness.ToolOutcome {
	result, err := f(ctx, call)
	out := harness.ToolOutcome{
		Result:   model.ToolResult{CallID: call.ID, Status: model.ResultSuccess, Content: result.Content},
		Metadata: result.Metadata, Evidence: harness.ExecutionObserved,
	}
	if err != nil {
		out.Metadata = nil
		out.Result.Status = model.ResultError
		out.Result.Content = strings.ToValidUTF8(err.Error(), "\uFFFD")
		if out.Result.Content == "" {
			out.Result.Content = "Tool call failed."
		}
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			out.Result.Status = model.ResultInterrupted
		}
		if errors.Is(err, ErrOutcomeUnknown) {
			out.Evidence = harness.ExecutionUnknown
		}
	}
	return out
}

// Activation supplies the entry point's settings and service calls.
func Activation(api API) AuthorContext {
	return AuthorContext{Settings: api.Settings(), Call: api.Call}
}

type FileStat struct {
	Path   string
	Exists bool
}

type Member struct {
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Starter string `json:"starter"`
}

func (c AuthorContext) invoke(ctx context.Context, service, method string, request, value any) error {
	args, err := json.Marshal(request)
	if err != nil {
		return err
	}
	return c.send(ctx, service, method, args, value)
}

// send makes one host call with already-encoded arguments, which go out
// unchanged.
func (c AuthorContext) send(ctx context.Context, service, method string, args json.RawMessage, value any) error {
	result, err := c.Call(ctx, service, method, args)
	if err != nil {
		return err
	}
	if result.Failure != nil {
		return &CallError{Err: fmt.Errorf("%s: %s", result.Failure.Code, result.Failure.Message), Partial: result.Failure.Partial}
	}
	if value != nil {
		return json.Unmarshal(result.Value, value)
	}
	return nil
}

func (c AuthorContext) StatFile(ctx context.Context, path string) (FileStat, error) {
	var result struct {
		Path   string `json:"canonical_path"`
		Exists bool   `json:"exists"`
	}
	err := c.invoke(ctx, ContractWorkspace, "stat", struct {
		Path string `json:"path"`
	}{path}, &result)
	return FileStat{Path: result.Path, Exists: result.Exists}, err
}

func (c AuthorContext) ReadFile(ctx context.Context, path string) ([]byte, error) {
	var result struct {
		Bytes []byte `json:"bytes"`
	}
	err := c.invoke(ctx, ContractWorkspace, "read", struct {
		Path string `json:"path"`
	}{path}, &result)
	return result.Bytes, err
}

func (c AuthorContext) writeFile(ctx context.Context, path string, content []byte, createOnly bool) error {
	err := c.invoke(ctx, ContractWorkspace, "write", struct {
		Path       string `json:"path"`
		Bytes      string `json:"bytes"`
		CreateOnly bool   `json:"create_only"`
	}{path, base64.StdEncoding.EncodeToString(content), createOnly}, nil)
	return uncertainMutation(err)
}

func (c AuthorContext) WriteFile(ctx context.Context, path string, content []byte) error {
	return c.writeFile(ctx, path, content, false)
}

func (c AuthorContext) CreateFile(ctx context.Context, path string, content []byte) error {
	return c.writeFile(ctx, path, content, true)
}

func (c AuthorContext) DeleteFile(ctx context.Context, path string) error {
	return uncertainMutation(c.invoke(ctx, ContractWorkspace, "delete", struct {
		Path string `json:"path"`
	}{path}, nil))
}

func uncertainMutation(err error) error {
	var callErr *CallError
	if errors.As(err, &callErr) {
		var partial struct {
			Outcome string `json:"outcome"`
		}
		if json.Unmarshal(callErr.Partial, &partial) == nil && partial.Outcome == "uncertain" {
			return fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
		}
	}
	return err
}

func (c AuthorContext) SetState(ctx context.Context, value json.RawMessage) error {
	return c.invoke(ctx, ContractState, "set", struct {
		Value json.RawMessage `json:"value"`
	}{value}, nil)
}

func (c AuthorContext) Members(ctx context.Context) ([]Member, error) {
	var members []Member
	err := c.invoke(ctx, ContractSession, "members", struct{}{}, &members)
	return members, err
}

func (c AuthorContext) StartSession(ctx context.Context, start SessionStart) (string, error) {
	args, err := EncodeSessionStart(start)
	if err != nil {
		return "", err
	}
	var value struct {
		SessionID string `json:"session_id"`
	}
	err = c.send(ctx, ContractSession, "start", args, &value)
	if err != nil {
		var callErr *CallError
		if errors.As(err, &callErr) {
			_ = json.Unmarshal(callErr.Partial, &value)
		}
		return value.SessionID, err
	}
	return value.SessionID, err
}

func (c AuthorContext) ReplaceSettings(ctx context.Context, section json.RawMessage) error {
	return c.invoke(ctx, ContractSettings, "replace", struct {
		Section json.RawMessage `json:"section"`
	}{section}, nil)
}

func (c AuthorContext) Credential(ctx context.Context, field string) (string, error) {
	var result struct {
		Value string `json:"value"`
	}
	err := c.invoke(ctx, ContractCredentials, "get", struct {
		Field string `json:"field"`
	}{field}, &result)
	return result.Value, err
}

func (c AuthorContext) directory(ctx context.Context, method string) (string, error) {
	var result struct {
		Directory string `json:"directory"`
	}
	err := c.invoke(ctx, ContractData, method, struct{}{}, &result)
	return result.Directory, err
}

func (c AuthorContext) SessionDataDir(ctx context.Context) (string, error) {
	return c.directory(ctx, "session_namespace")
}
func (c AuthorContext) CacheDir(ctx context.Context) (string, error) {
	return c.directory(ctx, "cache_namespace")
}
func (c AuthorContext) ClaimExternalData(ctx context.Context) error {
	return c.invoke(ctx, ContractData, "claim_external_data", struct{}{}, nil)
}
func (c AuthorContext) ReportWarning(ctx context.Context, kind, message string) error {
	return c.invoke(ctx, ContractWarnings, "report", struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
	}{kind, message}, nil)
}
func (c AuthorContext) Command(ctx context.Context, method string, request, result any) error {
	if request == nil {
		request = struct{}{}
	}
	return c.invoke(ctx, ContractCommands, method, request, result)
}
