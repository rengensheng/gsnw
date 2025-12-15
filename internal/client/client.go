package client

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/rengensheng/gsnw/internal/protocol"
	"github.com/rengensheng/gsnw/internal/server"
	"golang.org/x/term"
)

// Client represents a gsnw client
type Client struct {
	conn      net.Conn
	oldState  *term.State
	sessionName string
}

// Connect connects to the server
func Connect() (*Client, error) {
	socketPath := server.SocketPath()
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &Client{conn: conn}, nil
}

// Close closes the client connection
func (c *Client) Close() {
	if c.oldState != nil {
		term.Restore(int(os.Stdin.Fd()), c.oldState)
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

// NewSession creates a new session
func (c *Client) NewSession(name string) error {
	msg := protocol.NewMessage(protocol.MsgNewSession, []byte(name))
	if err := protocol.Write(c.conn, msg); err != nil {
		return err
	}

	resp, err := protocol.Read(c.conn)
	if err != nil {
		return err
	}

	if resp.Type == protocol.MsgError {
		return fmt.Errorf("server error: %s", string(resp.Payload))
	}

	c.sessionName = string(resp.Payload)
	return c.attachLoop()
}

// Attach attaches to an existing session
func (c *Client) Attach(name string) error {
	msg := protocol.NewMessage(protocol.MsgAttachSession, []byte(name))
	if err := protocol.Write(c.conn, msg); err != nil {
		return err
	}

	resp, err := protocol.Read(c.conn)
	if err != nil {
		return err
	}

	if resp.Type == protocol.MsgError {
		return fmt.Errorf("server error: %s", string(resp.Payload))
	}

	c.sessionName = string(resp.Payload)
	return c.attachLoop()
}

// List lists all sessions
func (c *Client) List() (string, error) {
	msg := protocol.NewMessage(protocol.MsgListSessions, nil)
	if err := protocol.Write(c.conn, msg); err != nil {
		return "", err
	}

	resp, err := protocol.Read(c.conn)
	if err != nil {
		return "", err
	}

	if resp.Type == protocol.MsgError {
		return "", fmt.Errorf("server error: %s", string(resp.Payload))
	}

	return string(resp.Payload), nil
}

// Kill kills a session
func (c *Client) Kill(name string) error {
	msg := protocol.NewMessage(protocol.MsgKillSession, []byte(name))
	return protocol.Write(c.conn, msg)
}

// attachLoop runs the main attach loop
func (c *Client) attachLoop() error {
	// Set terminal to raw mode
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return fmt.Errorf("failed to set raw mode: %w", err)
	}
	c.oldState = oldState

	// Send initial size
	c.sendSize()

	// Handle window resize
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGWINCH)
	go func() {
		for range sigCh {
			c.sendSize()
		}
	}()

	// Read from server and write to stdout
	go func() {
		for {
			msg, err := protocol.Read(c.conn)
			if err != nil {
				return
			}

			switch msg.Type {
			case protocol.MsgOutput:
				os.Stdout.Write(msg.Payload)
			case protocol.MsgDetachSession:
				fmt.Println("\r\n[detached]")
				c.Close()
				os.Exit(0)
			case protocol.MsgError:
				fmt.Fprintf(os.Stderr, "\r\nError: %s\r\n", string(msg.Payload))
			}
		}
	}()

	// Read from stdin and send to server
	buf := make([]byte, 1024)
	for {
		n, err := os.Stdin.Read(buf)
		if err != nil {
			return err
		}

		if n > 0 {
			msg := protocol.NewMessage(protocol.MsgInput, buf[:n])
			if err := protocol.Write(c.conn, msg); err != nil {
				return err
			}
		}
	}
}

// sendSize sends the current terminal size to the server
func (c *Client) sendSize() {
	width, height, err := term.GetSize(int(os.Stdin.Fd()))
	if err != nil {
		return
	}

	payload := protocol.EncodeResize(uint16(height), uint16(width))
	msg := protocol.NewMessage(protocol.MsgResize, payload)
	protocol.Write(c.conn, msg)
}

// IsServerRunning checks if the server is running
func IsServerRunning() bool {
	socketPath := server.SocketPath()
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// StartServer starts the server in the background
func StartServer() error {
	// Fork a new process to run the server
	exe, err := os.Executable()
	if err != nil {
		return err
	}

	// Start server process in background
	attr := &os.ProcAttr{
		Dir: "/",
		Env: os.Environ(),
		Files: []*os.File{
			nil, // stdin
			nil, // stdout
			nil, // stderr
		},
		Sys: &syscall.SysProcAttr{
			Setsid: true,
		},
	}

	_, err = os.StartProcess(exe, []string{exe, "server"}, attr)
	return err
}
