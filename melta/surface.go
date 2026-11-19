package melta

import (
	"context"
	"os"
	goruntime "runtime"
	"slices"
	"strings"
	"time"

	"github.com/Syntheresis/melta/harness"
	"github.com/Syntheresis/melta/model"
	"github.com/Syntheresis/melta/prompt"
)

// surfaceRequest is the captured pure input of one composed prompt and tool
// surface: the host-supplied identity and global rules directory, the
// preparation's Workspace and session-start environment, the selected
// definition's prompt fields, the captured Invocation, hard constraints, and
// session identity, and the compiled tool specs in the agent's selection
// order after first-occurrence dedupe.
type surfaceRequest struct {
	hostIdentity   string
	globalRulesDir string
	workspace      string
	sessionStart   time.Time
	promptSize     prompt.Mode
	promptBody     string
	invocation     Invocation
	model          model.ModelRef
	constraints    harness.ToolConstraints
	identity       harness.SessionIdentity
	agentInputs    harness.AgentInputs
	toolSpecs      []toolDeclaration
	shell          string
}

// surfaceResult is one composed prompt with its captured rule diagnostics and
// the exact baseline assembly inputs the prompt was rendered from.
type surfaceResult struct {
	Prompt   string
	Assembly prompt.AssemblyInput
	Warnings []prompt.Warning
}

type surfacePrompt struct {
	input     prompt.AssemblyInput
	globalDir string
	read      bool
	rules     prompt.Rules
	rulesErr  error
	assembled bool
	result    surfaceResult
	err       error
}

func newSurfacePrompt(req surfaceRequest) *surfacePrompt {
	start := req.sessionStart
	if start.IsZero() {
		start = time.Now()
	}
	input := prompt.AssemblyInput{
		Identity: req.hostIdentity, Instructions: req.promptBody, Mode: req.promptSize,
		Workspace: req.workspace, SessionStart: start,
	}
	if req.promptSize != prompt.ModeNone {
		input.OS = goruntime.GOOS
		input.Shell = req.shell
		input.OSDescription = captureOSDescription()
	}
	return &surfacePrompt{input: input, globalDir: req.globalRulesDir}
}

func (p *surfacePrompt) readRules() (prompt.Rules, error) {
	if !p.read {
		p.read = true
		p.rules, p.rulesErr = prompt.ReadRules(p.globalDir, p.input.Workspace, p.input.Mode)
	}
	rules, err := p.rules, p.rulesErr
	rules.Warnings = slices.Clone(rules.Warnings)
	return rules, err
}

func (p *surfacePrompt) sections() []prompt.Section {
	return prompt.BaselineSections(p.input)
}

func (p *surfacePrompt) environment() string {
	return prompt.Environment(p.input)
}

func (p *surfacePrompt) ruleSections() (string, []prompt.Section, error) {
	rules, err := p.readRules()
	if err != nil {
		return "", nil, err
	}
	return prompt.CombinedRules(rules), prompt.RuleSections(p.input.Mode, rules), nil
}

func (p *surfacePrompt) defaultPrompt() (surfaceResult, error) {
	if !p.assembled {
		p.assembled = true
		input := p.input
		input.Rules, p.err = p.readRules()
		if p.err != nil {
			return surfaceResult{}, p.err
		}
		text, err := prompt.Assemble(input)
		if err != nil {
			p.err = err
			return surfaceResult{}, p.err
		}
		p.result = surfaceResult{Prompt: text, Assembly: input, Warnings: input.Rules.Warnings}
	}
	result := p.result
	result.Warnings = slices.Clone(result.Warnings)
	result.Assembly.Rules.Warnings = slices.Clone(result.Assembly.Rules.Warnings)
	return result, p.err
}

func (p *surfacePrompt) resolve(replacement *string) (surfaceResult, error) {
	if replacement != nil {
		return surfaceResult{Prompt: *replacement, Warnings: slices.Clone(p.rules.Warnings)}, nil
	}
	return p.defaultPrompt()
}

// composeSurface converts the captured inputs into one final owned prompt and
// the eligible tool catalog, with no harness or storage I/O. The prompt reads
// the explicitly supplied global and Workspace rules and assembles the
// baseline through the public pure assembler. The catalog walks the tool specs
// in the agent's selection order after first-occurrence dedupe: every selected
// eligible tool is included — an exclusion can never revive a tool the hard
// constraints made ineligible. A zero session start samples the compose time
// once, so the resulting prompt is captured once.
func composeSurface(ctx context.Context, req surfaceRequest) (surfaceResult, []model.ToolDefinition, error) {
	result, err := newSurfacePrompt(req).defaultPrompt()
	if err != nil {
		return surfaceResult{}, nil, err
	}
	catalog := make([]model.ToolDefinition, 0, len(req.toolSpecs))
	for _, spec := range req.toolSpecs {
		description, err := spec.describe(ctx, req.invocation, req.model, req.constraints, req.identity, req.agentInputs)
		if err != nil {
			return surfaceResult{}, nil, err
		}
		if !description.Available {
			continue
		}
		catalog = append(catalog, description.Definition)
	}
	return result, catalog, nil
}

// captureOSDescription samples the platform description once per composed
// surface.
func captureOSDescription() string {
	if goruntime.GOOS == "linux" {
		if data, err := os.ReadFile("/etc/os-release"); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "PRETTY_NAME=") {
					name := strings.TrimPrefix(line, "PRETTY_NAME=")
					return strings.Trim(name, `"`)
				}
			}
		}
	}
	return goruntime.GOOS + "/" + goruntime.GOARCH
}
