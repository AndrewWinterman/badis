package cluster

import (
	"bytes"
	"io"
	"testing"
)

func TestResolveOptionsDefaultsToDiscard(t *testing.T) {
	opts := resolveOptions(nil)

	if opts.logOutput != io.Discard {
		t.Fatalf("expected default log output to be io.Discard")
	}
}

func TestResolveOptionsUsesProvidedLogOutput(t *testing.T) {
	var buf bytes.Buffer
	opts := resolveOptions([]Option{WithLogOutput(&buf)})

	if opts.logOutput != &buf {
		t.Fatalf("expected custom log output writer to be used")
	}
}
