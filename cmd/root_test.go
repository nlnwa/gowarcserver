package cmd

import (
	"testing"
)

func TestRootCmd(t *testing.T) {
	rootCmd := NewCommand()
	err := rootCmd.Execute()
	if err != nil {
		t.Errorf("%v", err)
	}
}
