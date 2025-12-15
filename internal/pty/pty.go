package pty

import (
	"os"
	"os/exec"

	"github.com/creack/pty"
)

// PTY wraps a pseudo-terminal
type PTY struct {
	file *os.File
	cmd  *exec.Cmd
}

// New creates a new PTY running the given shell
func New(shell string) (*PTY, error) {
	if shell == "" {
		shell = os.Getenv("SHELL")
		if shell == "" {
			shell = "/bin/sh"
		}
	}

	cmd := exec.Command(shell)
	cmd.Env = append(os.Environ(), "TERM=xterm-256color")

	ptmx, err := pty.Start(cmd)
	if err != nil {
		return nil, err
	}

	return &PTY{
		file: ptmx,
		cmd:  cmd,
	}, nil
}

// File returns the PTY file descriptor
func (p *PTY) File() *os.File {
	return p.file
}

// Read reads from the PTY
func (p *PTY) Read(b []byte) (int, error) {
	return p.file.Read(b)
}

// Write writes to the PTY
func (p *PTY) Write(b []byte) (int, error) {
	return p.file.Write(b)
}

// Resize changes the terminal size
func (p *PTY) Resize(rows, cols uint16) error {
	return pty.Setsize(p.file, &pty.Winsize{
		Rows: rows,
		Cols: cols,
	})
}

// Close closes the PTY and terminates the process
func (p *PTY) Close() error {
	if p.cmd != nil && p.cmd.Process != nil {
		p.cmd.Process.Kill()
		p.cmd.Wait()
	}
	return p.file.Close()
}

// Wait waits for the command to exit
func (p *PTY) Wait() error {
	return p.cmd.Wait()
}

// Done returns a channel that closes when the process exits
func (p *PTY) Done() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		p.cmd.Wait()
		close(ch)
	}()
	return ch
}
