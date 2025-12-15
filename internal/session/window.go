package session

import (
	"sync"

	"github.com/rengensheng/gsnw/internal/pty"
)

// Window represents a terminal window
type Window struct {
	ID   int
	Name string
	pty  *pty.PTY

	mu     sync.Mutex
	closed bool
}

// NewWindow creates a new window with a PTY
func NewWindow(id int, shell string) (*Window, error) {
	p, err := pty.New(shell)
	if err != nil {
		return nil, err
	}

	return &Window{
		ID:   id,
		Name: "",
		pty:  p,
	}, nil
}

// PTY returns the window's PTY
func (w *Window) PTY() *pty.PTY {
	return w.pty
}

// Write sends input to the window
func (w *Window) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, nil
	}
	return w.pty.Write(data)
}

// Read reads output from the window
func (w *Window) Read(buf []byte) (int, error) {
	return w.pty.Read(buf)
}

// Resize changes the window dimensions
func (w *Window) Resize(rows, cols uint16) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	return w.pty.Resize(rows, cols)
}

// Close closes the window
func (w *Window) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	return w.pty.Close()
}

// Done returns a channel that closes when the window's process exits
func (w *Window) Done() <-chan struct{} {
	return w.pty.Done()
}
