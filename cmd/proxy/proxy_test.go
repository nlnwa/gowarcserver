package proxy

import (
	"testing"
)

func TestIndexCmd(t *testing.T) {
	cmd := NewCommand()
	cmd.SetArgs([]string{"-h"})
	err := cmd.Execute()
	if err != nil {
		t.Errorf("%v", err)
	}
}
