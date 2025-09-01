package plan

import (
	"fmt"
	"sort"
	"strings"
)

// Render writes the plan as the text shown on the deployment page.
//
// The markers are chosen so a reader can scan the left column: "+" adds, "~"
// changes something reversible, "!" changes something that cannot be undone,
// "-" destroys data, "S" touches the registry, and "x" is refused.
func (p *Plan) Render() string {
	if p.Empty() {
		return "No changes. The cluster already matches the desired state."
	}

	var b strings.Builder

	if len(p.Changes) > 0 {
		fmt.Fprintf(&b, "Plan: %s.\n\n", summarise(p.Changes))
		for _, change := range p.Changes {
			fmt.Fprintf(&b, "  %s %s\n", marker(change), change.Describe())
		}
	}

	if irreversible := p.Irreversible(); len(irreversible) > 0 {
		b.WriteString("\nThese changes cannot be undone by a rollback:\n")
		for _, change := range irreversible {
			fmt.Fprintf(&b, "  %s %s\n", marker(change), change.Describe())
		}
	}

	if len(p.Blocked) > 0 {
		fmt.Fprintf(&b, "\nBlocked (%d). Nothing will be applied until these are resolved:\n", len(p.Blocked))
		for _, blocked := range p.Blocked {
			fmt.Fprintf(&b, "  x %s\n", blocked.Reason)
		}
	}

	return b.String()
}

func marker(c Change) string {
	switch c.Kind {
	case CreateTopic:
		return "+"
	case UpdateTopicConfig:
		return "~"
	case IncreasePartitions:
		return "!"
	case DeleteTopic:
		return "-"
	case RegisterSchema:
		return "S"
	default:
		return "?"
	}
}

// summarise counts the changes by kind, in a stable order.
func summarise(changes []Change) string {
	counts := map[ChangeKind]int{}
	for _, change := range changes {
		counts[change.Kind]++
	}

	kinds := make([]ChangeKind, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })

	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, fmt.Sprintf("%d to %s", counts[kind], kind))
	}
	return strings.Join(parts, ", ")
}
