package extension

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// ErrInvalid marks every rejected manifest, schema, method or value shape.
var ErrInvalid = errors.New("extension: invalid declaration")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// CompiledMethod is one compiled method declaration.
type CompiledMethod struct {
	Method
	input   *jsonschema.Schema
	success *jsonschema.Schema
	failure *jsonschema.Schema
	partial *jsonschema.Schema
}

func compileMethod(method Method) (*CompiledMethod, error) {
	label := method.Name
	input, err := compileSchema(method.Input, label+"/input")
	if err != nil {
		return nil, invalid("method %q input schema: %v", method.Name, err)
	}
	success, err := compileSchema(method.Success, label+"/success")
	if err != nil {
		return nil, invalid("method %q success schema: %v", method.Name, err)
	}
	var failure, partial *jsonschema.Schema
	if len(method.Error) != 0 {
		failure, err = compileSchema(method.Error, label+"/error")
		if err != nil {
			return nil, invalid("method %q error schema: %v", method.Name, err)
		}
	}
	if len(method.Partial) != 0 {
		partial, err = compileSchema(method.Partial, label+"/partial")
		if err != nil {
			return nil, invalid("method %q partial schema: %v", method.Name, err)
		}
	}
	return &CompiledMethod{Method: method, input: input, success: success, failure: failure, partial: partial}, nil
}

// ValidateArguments validates arguments against this method.
func (contract *CompiledMethod) ValidateArguments(arguments json.RawMessage) error {
	return validateValue(contract.input, arguments, "arguments")
}

// ValidateResultEnvelope checks one result's common union envelope: exactly
// one success value or failure arm and nonempty failure code/message. Payload
// bytes stay opaque; their schema owners validate the enclosed values.
func ValidateResultEnvelope(result Result) error {
	if (result.Failure == nil) == (result.Value == nil) {
		return errors.New("result must carry exactly one success value or failure")
	}
	if result.Failure != nil && (result.Failure.Code == "" || result.Failure.Message == "") {
		return errors.New("failure requires nonempty code and message")
	}
	return nil
}

// ValidateResult validates the result against this method.
func (contract *CompiledMethod) ValidateResult(result Result) error {
	method := contract.Name
	if err := ValidateResultEnvelope(result); err != nil {
		return invalid("method %q: %v", method, err)
	}
	if result.Failure != nil {
		if len(result.Failure.Data) != 0 {
			if contract.failure == nil {
				return invalid("method %q failure data is not declared", method)
			}
			if err := validateValue(contract.failure, result.Failure.Data, "failure data"); err != nil {
				return err
			}
		}
		if len(result.Failure.Partial) != 0 {
			if contract.partial == nil {
				return invalid("method %q partial result is not declared", method)
			}
			if err := validateValue(contract.partial, result.Failure.Partial, "partial result"); err != nil {
				return err
			}
		}
		return nil
	}
	return validateValue(contract.success, result.Value, "success value")
}

func validateSection(id string, schema *jsonschema.Schema, section json.RawMessage) error {
	value, err := decodeJSON(section)
	if err != nil {
		return invalid("extension %q section: %v", id, err)
	}
	if err := schema.Validate(value); err != nil {
		return invalid("extension %q section: %v", id, err)
	}
	return nil
}

func validateValue(schema *jsonschema.Schema, raw json.RawMessage, label string) error {
	value, err := decodeJSON(raw)
	if err != nil {
		return invalid("%s: %v", label, err)
	}
	if err := schema.Validate(value); err != nil {
		return invalid("%s: %v", label, err)
	}
	return nil
}

// schemaCacheKey identifies one compiled schema by its exact resource label
// and exact schema bytes.
type schemaCacheKey struct {
	label string
	raw   string
}

// compiledSchemas memoizes successful compilations process-wide. A compiled
// *jsonschema.Schema is fully built by Compiler.Compile and validated only
// through per-call validator state, so it is immutable after compilation and
// safe for concurrent Validate. Failed compilations are never cached.
var compiledSchemas sync.Map // schemaCacheKey -> *jsonschema.Schema

// compileSchema compiles one local schema. Relative references resolve only
// inside the supplied document; a remote reference reaches the rejecting
// loader and fails. The compiler URL is private to this compile call, and
// identical label and bytes reuse one compiled schema.
func compileSchema(raw json.RawMessage, label string) (*jsonschema.Schema, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, errors.New("missing schema")
	}
	key := schemaCacheKey{label: label, raw: string(raw)}
	if cached, ok := compiledSchemas.Load(key); ok {
		return cached.(*jsonschema.Schema), nil
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(rejectingLoader{})
	const location = "melta:///local-schema"
	if err := compiler.AddResource(location, document); err != nil {
		return nil, err
	}
	schema, err := compiler.Compile(location)
	if err != nil {
		return nil, err
	}
	compiledSchemas.Store(key, schema)
	return schema, nil
}

type rejectingLoader struct{}

func (rejectingLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("external schema reference %q is not permitted", url)
}

// decodeJSON decodes one JSON value losslessly: numbers keep their lexemes and
// object member order is immaterial. An empty value is invalid.
func decodeJSON(raw []byte) (any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("missing JSON value")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON value")
		}
		return nil, fmt.Errorf("trailing JSON input: %w", err)
	}
	return value, nil
}
