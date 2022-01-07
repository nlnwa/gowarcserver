package index

import (
	"testing"
)

func TestIndexCmd(t *testing.T) {
	cmd := NewCommand()
	err := cmd.Execute()
	if err != nil {
		t.Errorf("%v", err)
	}
}
