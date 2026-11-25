package tool

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"
)

const (
	maxInt = int64(^uint(0) >> 1)
	minInt = -maxInt - 1
)

func jsonNumberToInt(value any, tool, key string) (int, error) {
	n, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("%s: %s must be an integer", tool, key)
	}
	s := string(n)
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		if i > maxInt || i < minInt {
			return 0, fmt.Errorf("%s: %s is too large", tool, key)
		}
		return int(i), nil
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok || !r.IsInt() {
		return 0, fmt.Errorf("%s: %s must be an integer", tool, key)
	}
	if !r.Num().IsInt64() || r.Num().Int64() > maxInt || r.Num().Int64() < minInt {
		return 0, fmt.Errorf("%s: %s is too large", tool, key)
	}
	return int(r.Num().Int64()), nil
}

func ReadArgs(args map[string]any, defaultLimit int) (path string, offset, limit int, err error) {
	path, _ = args["path"].(string)
	if path == "" {
		err = fmt.Errorf("read_file: path is required")
		return
	}
	offset, limit = 1, defaultLimit
	if value, ok := args["offset"]; ok {
		offset, err = jsonNumberToInt(value, "read_file", "offset")
		if err != nil {
			return
		}
		if offset < 1 {
			offset = 1
		}
	}
	if value, ok := args["limit"]; ok {
		limit, err = jsonNumberToInt(value, "read_file", "limit")
		if err != nil {
			return
		}
		if limit < 1 {
			limit = defaultLimit
		}
	}
	return
}

func WriteArgs(args map[string]any) (path, content string, err error) {
	path, _ = args["path"].(string)
	content, _ = args["content"].(string)
	if path == "" {
		err = fmt.Errorf("write_file: path is required")
	}
	return
}

func EditArgs(args map[string]any) (path, oldString, newString string, replaceAll bool, err error) {
	path, _ = args["path"].(string)
	oldString, _ = args["old_string"].(string)
	newString, _ = args["new_string"].(string)
	replaceAll, _ = args["replace_all"].(bool)
	if path == "" {
		err = fmt.Errorf("edit_file: path is required")
	}
	return
}

func RunCommandArgs(args map[string]any, defaultTimeout int) (command string, timeout int, background bool, err error) {
	command, _ = args["command"].(string)
	background, _ = args["background"].(bool)
	if command == "" {
		err = fmt.Errorf("run_command: command is required")
		return
	}
	timeout = defaultTimeout
	if background {
		timeout = 0
	}
	if value, ok := args["timeout"]; ok {
		var v int
		v, err = jsonNumberToInt(value, "run_command", "timeout")
		if err != nil {
			return
		}
		if int64(v) > int64(math.MaxInt64/time.Second) {
			err = fmt.Errorf("run_command: timeout is too large")
			return
		}
		if v >= 1 {
			timeout = v
		}
	}
	return
}

func ProcessArgs(args map[string]any) (action, id string, err error) {
	action, _ = args["action"].(string)
	id, _ = args["id"].(string)
	switch action {
	case "list":
	case "read", "kill":
		if id == "" {
			err = fmt.Errorf("process: id is required for %s", action)
			return
		}
		if len(id) != 8 {
			err = fmt.Errorf("process: no process with ID %q", id)
			return
		}
		for i := range id {
			c := id[i]
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				err = fmt.Errorf("process: no process with ID %q", id)
				return
			}
		}
	default:
		err = fmt.Errorf("process: unknown action %q", action)
	}
	return
}
