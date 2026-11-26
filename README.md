<h1 align="center">Melta</h1>

<p align="center">
  A coding agent that works with any OpenAI-compatible LLM provider.<br>
  Desktop app · Terminal CLI · ACP stdio adapter — one Go binary, three interfaces.
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"></a>
  <a href="https://goreportcard.com/report/github.com/Syntheresis/melta"><img src="https://goreportcard.com/badge/github.com/Syntheresis/melta" alt="Go Report Card"></a>
</p>

---

### Goals

| | |
|---|---|
| **Model freedom** | No vendor lock-in — any OpenAI-compatible endpoint |
| **Simplicity** | One binary, minimal moving parts, no external services |
| **Reliability** | Reversion and snapshots that don't break |
| **Economics** | Bring your own providers, mix cloud and local |

---

### Features

**Providers** — Bundled metadata for OpenAI, OpenRouter, Alibaba/Qwen, Google Gemini, xAI/Grok, DeepSeek, Moonshot/Kimi, Mistral, MiniMax, Z.ai, Together, Groq, Fireworks, and Xiaomi MiMo. Add Ollama, llama.cpp, or any other OpenAI-compatible endpoint through config. Configure N providers simultaneously, switch mid-session.

Melta uses the OpenAI Chat Completions shape with streaming and tool calls. Provider compatibility still varies: some OpenAI-compatible models stream text correctly but do not reliably support streamed tool calls. Test a new provider/model with a real tool call before relying on it.

**Tools** — `read_file` · `write_file` · `edit_file` · `run_command` · `process` · `task`; Adaptation supplies `apply_patch` for models that support it

**Snapshots** — File edits are captured copy-based before they happen, grouped per admitted operation. Revert code at any owned input or fork the conversation from it. No git dependency.

**Context compaction** — Automatic single-request summarization over the frozen conversation when provider-reported usage would exceed the model's context window; `/compact` triggers it manually.

**Subagents** — Delegate a task to a background child session with its own context, tool surface, and model; concurrent child sessions are bounded by configuration.

---

### Quick start

#### Install

Existing releases still contain Lightcode. Until a Melta release is published, build from source using the
instructions below.

Linux amd64 is the first supported release target. Install the prebuilt binary from the latest GitHub Release:

```bash
curl -fsSL https://github.com/Syntheresis/melta/releases/latest/download/install.sh | sh
```

Run Melta:

```bash
melta
```

Linux amd64 on Debian/Ubuntu and Fedora is the first supported release target. macOS, Windows, Linux arm64, and package-manager formats are not part of this first release target.

#### Build from source

Source builds are for development. They require Go 1.26+, Node.js, [Wails v2](https://wails.io/docs/gettingstarted/installation), and WebKitGTK development headers.

```bash
# Debian / Ubuntu build dependencies
sudo apt update
sudo apt install libgtk-3-dev libwebkit2gtk-4.1-dev
```

```bash
wails build -tags webkit2_41
```

Binary: `build/bin/melta`

Optional install:

```bash
install -Dm755 build/bin/melta ~/.local/bin/melta
```

#### Configure

Melta ships with bundled provider metadata, but no credentials and no preset default model. On first run it
creates `~/.melta/config.json` when absent, enabling the shipped Agents and Adaptation extensions without
selecting a model. API keys live in environment variables (or `~/.melta/.env`), referenced by name from
provider `transport.api_key_env` fields:

```json
{
  "model": "ollama/qwen3.6:27b",
  "providers": {
    "openrouter": {
      "transport": {
        "headers": {
          "HTTP-Referer": "https://my-app.example"
        }
      },
      "models": {
        "z-ai/glm-5.1": {
          "name": "Z.ai GLM-5.1"
        }
      }
    },
    "ollama": {
      "transport": {
        "base_url": "http://localhost:11434/v1",
        "api_key_env": ""
      },
      "models": {
        "qwen3.6:27b": {
          "name": "Qwen3.6 27B",
          "context_window": 262144,
          "max_output_tokens": 65536
        }
      }
    }
  }
}
```

The top-level `model` is the baseline choice, including when no extensions
are enabled. A Session's chosen model takes precedence; if it cannot bind,
Melta tries this baseline. Named Agent definitions can select their own models
in the same `~/.melta/config.json` document:

```json
{
  "model": "openrouter/z-ai/glm-5.1",
  "extensions": {
    "agents": {
      "enabled": true,
      "definitions": {
        "primary": {"model": "openrouter/z-ai/glm-5.1"}
      }
    },
    "adaptation": {"enabled": true}
  }
}
```

Example key setup:

```bash
mkdir -p ~/.melta
printf 'OPENROUTER_API_KEY=...\n' >> ~/.melta/.env
```

Shell-exported environment variables take precedence over values in `~/.melta/.env`.

#### File access

File tools reach any path the operating-system user can reach, inside or outside the Workspace, including `.env`, key and credential files and Melta's own data directory. An extension's file operations through Melta have the same reach. Commands always could.

#### Run

Run Melta from the workspace directory you want it to work on:

```bash
melta                  # Desktop GUI
melta cli              # Terminal REPL
melta acp              # JSON-RPC over stdio
```

All three clients share one managed Runtime owner process. The first client starts it
(`melta runtime --data-dir ~/.melta --config ~/.melta/config.json` under the hood) and
later clients attach to the same owner. SIGINT or SIGTERM on the Runtime process is the managed
shutdown — closing the last client is not a shutdown. `melta upgrade` replaces the executable,
not the running Runtime; restart the Runtime to apply backend changes.

Useful CLI commands:

- `/help` — command reference
- `/new`, `/resume`, `/session` — start, resume, or switch sessions
- `/model`, `/agent` — switch model or agent type
- `/project` — switch workspace
- `/revert`, `/fork` — revert code at a turn, or fork from it
- `/context`, `/compact`, `/warnings` — token usage, manual compaction, current warnings
- `/children`, `/stop`, `/runtime`, `/copy`, `/clear`, `/exit`

ACP clients drive Melta over newline-delimited JSON-RPC on stdio.

#### Commands

Beyond the run modes above, the binary ships utility commands: `runtime` (run the shared Runtime
owner directly; clients start it automatically), `version`, `doctor` (offline install/config
diagnostics), `upgrade` (self-update; verifies the release's SHA-256 checksum before installing),
`uninstall`, `completion`, `models`, and `config`. Run `melta help` for the full reference.

#### Data locations

- `~/.melta/config.json` — provider, baseline and extension settings, including Agent definitions
- `~/.melta/.env` — local API keys
- `~/.melta/melta.db` — durable session state (SQLite), owned by the managed owner
- `~/.melta/runtime.json` — live Runtime owner record (present only while the owner runs)
- `~/.melta/code/<session-id>/` — per-session code snapshots and command output (internal adjunct)
- `~/.melta/cache/discovery/` — provider discovery responses
- `~/.melta/debug/wire/` — raw request and stream dumps when `MELTA_DEBUG_WIRE=1`

Filesystem sessions created before the SQLite store stay under `~/.lightcode/projects/`: no reader enumerates,
loads, migrates, repairs, or deletes their conversation or snapshot bytes. The current protocol has no retained
snapshot operations for them.

### Framework and extension API

The same source tree exposes the public Go framework used by the shipped runtime. The Engine is independently
usable: [`model/`](model/) defines provider-neutral model values and the OpenAI-compatible one-attempt transport;
[`agent/`](agent/) provides stream assembly, the model/tool loop and the standalone adapters. A zero-tool run is
valid, and Engine-only callers own persistence and recovery. See
[`melta/testdata/external/engine/engine_test.go`](melta/testdata/external/engine/engine_test.go)
(`TestEngineOnlyRun`).

The complete baseline is [`melta/`](melta/): `melta.Open(ctx, Options)` in [`melta/melta.go`](melta/melta.go)
composes the public [`harness/`](harness/) state machine, [`prompt/`](prompt/) assembler, standard tools, jobs and
included SQLite storage. `DataDir`, `Configuration` and `Credentials` are required and explicit; the locations for
global rules, discovery cache and wire debugging are optional, as are `Defaults` (Session input
defaults, including the host model), `Strategy`, native `Extensions` and the storage factory. An empty `Extensions` slice with no installed packages is the supported zero-extension baseline: the
five standard tools, SQLite storage, jobs and default prompt/rules handling are built in. The Agents and Adaptation
extensions are optional; Adaptation also supplies the multi-file `apply_patch` tool for matching models. Creating a Session requires a non-empty local Workspace; no optional extension or network
listener is required.
[`extension/`](extension/) is the activation-owned native contract (`extension.API`, typed callbacks and fixed host
services), and [`extension/worker/`](extension/worker/) is the executable extension protocol. The running owner and
the public worker entry both use the author `worker.Serve` protocol at API version 10. [`runtime/`](runtime/) is only
the protocol host and first-run initialization; [`protocol/`](protocol/) is the generated contract and
[`client/`](client/) the handwritten HTTP/SSE client.

An extension can register a tool as one function. This example requests `melta.workspace` in its manifest and
appends a line with snapshot capture:

```go
func Activate(ctx context.Context, api extension.API) (func(context.Context) error, error) {
	_, err := api.RegisterTool(model.ToolDefinition{
		Name:        "append_note",
		Description: "Append a line to notes.md in the workspace.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`),
	}, func(ctx context.Context, call extension.ToolCall) (extension.ToolResult, error) {
		var args struct{ Text string `json:"text"` }
		if err := call.Decode(&args); err != nil {
			return extension.ToolResult{}, err
		}
		stat, err := call.StatFile(ctx, "notes.md")
		if err != nil {
			return extension.ToolResult{}, err
		}
		var old []byte
		if stat.Exists {
			old, err = call.ReadFile(ctx, "notes.md")
			if err != nil {
				return extension.ToolResult{}, err
			}
		}
		if err := call.WriteFile(ctx, "notes.md", append(old, args.Text+"\n"...)); err != nil {
			return extension.ToolResult{}, err
		}
		return extension.ToolResult{Content: "Added."}, nil
	}, nil)
	return nil, err
}
```

Extensions use the registered activation surface. `ListExtensions` returns every enabled extension with its
version, optional configuration schema, settings, registered tools, client operations and inactive reason;
`InvokeExtension` names the extension, operation, harness/workspace/session target, arguments and request ID, so
adding an extension needs no protocol regeneration. A declared success returns the success envelope with its exact
raw result; a validated declared failure returns HTTP 422 with the extension's own code/message and optional
data/partial, not a baseline error code; predispatch and host failures use the normal typed error codes and statuses.
The CLI (`/runtime ...`), ACP (`runtime/...`) and desktop bridge all forward these operations through the shared
generated invocation table. There is no specialized extension UI. Work done for a client or Operation goes through
that callback's own `AuthorContext.Call`, within its limits. An activation can issue an independent attributed
Session command through `extension.API.Command` for work it does on its own; a context Melta handed to a callback
is denied, a nil context is treated as independent, and observer delivery carries no callback mark. The command
runs the ordinary producer under the activation's granted services and scope.

Local executable extensions are installed as a directory containing `manifest.json` and a contained entry point;
install and remove change next-start selection only and edit neither configuration nor Session data. The loaded code
set is fixed until the owner restarts, so replacing executable code needs a deliberate restart, not a Runtime
rebuild. Settings and enablement changes use ordinary configuration reload.

Tracked public acceptance examples are
[`melta/testdata/external/baseline_test.go`](melta/testdata/external/baseline_test.go)
(`TestPublicBaselineEndToEnd`, zero extensions),
[`melta/testdata/external/extension_test.go`](melta/testdata/external/extension_test.go)
(`TestPublicExtensionIntegration`), and [`melta/packages_test.go`](melta/packages_test.go)
(`TestInstalledWorkerPublicExtensionAcceptance`). The independent module runs under
`TestExternalModuleGOWORKOff`; `go test ./...` includes it. Other structural gates are
`go test ./agent -run TestPublicFoundationDependencyIsolation` and `make check-protocol`; the frontend suite runs
with `cd frontend && npm test`.

---

### License

[MIT](LICENSE)
