package server

import "io"

type options struct {
	logOutput io.Writer
}

// Option configures server behavior.
type Option func(*options)

// WithLogOutput configures raft and internal server log output.
func WithLogOutput(w io.Writer) Option {
	return func(opts *options) {
		opts.logOutput = w
	}
}

func resolveOptions(opts []Option) options {
	resolved := options{
		logOutput: io.Discard,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}

	if resolved.logOutput == nil {
		resolved.logOutput = io.Discard
	}

	return resolved
}
