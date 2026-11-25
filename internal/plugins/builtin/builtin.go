// Package builtin assembles Melta's shipped optional native extension
// registration set: the bundled model adaptation and the Agents extension
// (definitions, configuration edit, and child-session task tool). The durable
// SQLite store, the jobs service, and the five standard tools are baseline
// services the Melta owner constructs directly, so they are not part of
// this set. It is ordinary composition data with no configuration logic and
// no additional Core authority; a custom build supplies its own native slice
// through the same melta.Open.
package builtin

import (
	"github.com/Syntheresis/melta/extension"
	"github.com/Syntheresis/melta/extensions/adaptation"
	"github.com/Syntheresis/melta/extensions/agents"
)

// Plugins returns the shipped registration set in registration order.
func Plugins() []extension.Extension {
	return []extension.Extension{adaptation.Author(), agents.Author()}
}
