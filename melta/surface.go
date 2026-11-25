package melta

import (
	"os"
	goruntime "runtime"
	"slices"
	"strings"
	"time"

	"github.com/Syntheresis/melta/prompt"
)

// surfaceRequest is the captured pure input of one composed prompt surface:
// the host-supplied identity and global rules directory, and the preparation's
// Workspace and session-start environment.
type surfaceRequest struct {
	hostIdentity   string
	globalRulesDir string
	workspace      string
	sessionStart   time.Time
	promptBody     string
	shell          string
}

// surfaceResult is one composed prompt with its captured rule diagnostics.
type surfaceResult struct {
	Prompt   string
	Warnings []prompt.Warning
}

type surfacePrompt struct {
	input     prompt.AssemblyInput
	globalDir string
	read      bool
	rules     prompt.Rules
	assembled bool
	result    surfaceResult
}

func newSurfacePrompt(req surfaceRequest) *surfacePrompt {
	start := req.sessionStart
	if start.IsZero() {
		start = time.Now()
	}
	input := prompt.AssemblyInput{
		Identity: req.hostIdentity, Instructions: req.promptBody,
		Workspace: req.workspace, SessionStart: start,
		OS: goruntime.GOOS, Shell: req.shell, OSDescription: captureOSDescription(),
	}
	return &surfacePrompt{input: input, globalDir: req.globalRulesDir}
}

func (p *surfacePrompt) readRules() prompt.Rules {
	if !p.read {
		p.read = true
		p.rules = prompt.ReadRules(p.globalDir, p.input.Workspace)
	}
	rules := p.rules
	rules.Warnings = slices.Clone(rules.Warnings)
	return rules
}

func (p *surfacePrompt) sections() []prompt.Section {
	return prompt.BaselineSections(p.input)
}

func (p *surfacePrompt) environment() string {
	return prompt.Environment(p.input)
}

func (p *surfacePrompt) ruleSections() (string, []prompt.Section, error) {
	rules := p.readRules()
	return prompt.CombinedRules(rules), prompt.RuleSections(rules), nil
}

func (p *surfacePrompt) defaultPrompt() (surfaceResult, error) {
	if !p.assembled {
		p.assembled = true
		input := p.input
		input.Rules = p.readRules()
		p.result = surfaceResult{Prompt: prompt.Assemble(input), Warnings: input.Rules.Warnings}
	}
	result := p.result
	result.Warnings = slices.Clone(result.Warnings)
	return result, nil
}

func (p *surfacePrompt) resolve(replacement *string) (surfaceResult, error) {
	if replacement != nil {
		return surfaceResult{Prompt: *replacement, Warnings: slices.Clone(p.rules.Warnings)}, nil
	}
	return p.defaultPrompt()
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
