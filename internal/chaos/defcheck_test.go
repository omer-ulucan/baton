//go:build chaos

package chaos

import (
	"testing"

	"github.com/omer-ulucan/baton/internal/workflow"
)

func TestDefinitionsParse(t *testing.T) {
	for name, src := range definitions {
		if _, err := workflow.Parse([]byte(src)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
