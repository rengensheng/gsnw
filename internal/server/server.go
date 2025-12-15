package server

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/rengensheng/gsnw/internal/protocol"
	"github.com/rengensheng/gsnw/internal/session"
)

// Server is the gsnw server
type Server struct {
	socketPath string
	listener   net.Listener
	manager    *session.Manager

	mu      sync.Mutex
	clients map[net.Conn]*ClientState

	shutdownCh chan struct{}
	shutdown   bool
}

// ClientState tracks per-client state
type ClientState struct {
	session   *session.Session
	outputCh  chan []byte
	stopCh    chan struct{}
	prefixKey bool
	writeMu   sync.Mutex // protects conn writes
}

// New creates a new server
func New() *Server {
	return &Server{
		manager:    session.NewManager(),
		clients:    make(map[net.Conn]*ClientState),
		shutdownCh: make(chan struct{}),
	}
}

// SocketPath returns the default socket path
func SocketPath() string {
	uid := os.Getuid()
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("gsnw-%d", uid))
	os.MkdirAll(dir, 0700)
	return filepath.Join(dir, "default.sock")
}

// Start starts the server
func (s *Server) Start() error {
	s.socketPath = SocketPath()

	// Remove existing socket file
	os.Remove(s.socketPath)

	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = listener

	fmt.Printf("Server listening on %s\n", s.socketPath)

	// Handle shutdown in separate goroutine
	go func() {
		<-s.shutdownCh
		s.listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			isShutdown := s.shutdown
			s.mu.Unlock()
			if isShutdown {
				fmt.Println("Server shutting down...")
				os.Remove(s.socketPath)
				return nil
			}
			return err
		}
		go s.handleClient(conn)
	}
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown() {
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return
	}
	s.shutdown = true
	s.mu.Unlock()
	close(s.shutdownCh)
}

// checkAutoShutdown checks if all sessions are gone and shuts down if so
func (s *Server) checkAutoShutdown() {
	if s.manager.Count() == 0 {
		s.Shutdown()
	}
}

// handleClient handles a single client connection
func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()

	for {
		msg, err := protocol.Read(conn)
		if err != nil {
			s.detachClient(conn)
			return
		}

		if err := s.handleMessage(conn, msg); err != nil {
			errMsg := protocol.NewMessage(protocol.MsgError, []byte(err.Error()))
			s.writeToConn(conn, errMsg)
		}
	}
}

// writeToConn safely writes to a connection using the client's write lock if available
func (s *Server) writeToConn(conn net.Conn, msg *protocol.Message) error {
	s.mu.Lock()
	state := s.clients[conn]
	s.mu.Unlock()

	if state != nil {
		state.writeMu.Lock()
		defer state.writeMu.Unlock()
	}
	return protocol.Write(conn, msg)
}

// handleMessage processes a single message
func (s *Server) handleMessage(conn net.Conn, msg *protocol.Message) error {
	switch msg.Type {
	case protocol.MsgNewSession:
		return s.handleNewSession(conn, msg)
	case protocol.MsgAttachSession:
		return s.handleAttachSession(conn, msg)
	case protocol.MsgDetachSession:
		return s.handleDetachSession(conn)
	case protocol.MsgListSessions:
		return s.handleListSessions(conn)
	case protocol.MsgKillSession:
		return s.handleKillSession(msg)
	case protocol.MsgInput:
		return s.handleInput(conn, msg)
	case protocol.MsgResize:
		return s.handleResize(conn, msg)
	case protocol.MsgNewWindow:
		return s.handleNewWindow(conn)
	case protocol.MsgSelectWindow:
		return s.handleSelectWindow(conn, msg)
	case protocol.MsgKillWindow:
		return s.handleKillWindow(conn)
	default:
		return fmt.Errorf("unknown message type: %d", msg.Type)
	}
}

func (s *Server) handleNewSession(conn net.Conn, msg *protocol.Message) error {
	name := string(msg.Payload)
	if name == "" {
		name = fmt.Sprintf("session-%d", len(s.manager.List()))
	}

	sess, err := s.manager.Create(name)
	if err != nil {
		return err
	}

	// Send success response BEFORE starting goroutines
	resp := protocol.NewMessage(protocol.MsgSuccess, []byte(name))
	if err := protocol.Write(conn, resp); err != nil {
		return err
	}

	s.attachSession(conn, sess)
	return nil
}

func (s *Server) handleAttachSession(conn net.Conn, msg *protocol.Message) error {
	name := string(msg.Payload)

	sess, err := s.manager.Get(name)
	if err != nil {
		return err
	}

	// Send success response BEFORE starting goroutines
	resp := protocol.NewMessage(protocol.MsgSuccess, []byte(name))
	if err := protocol.Write(conn, resp); err != nil {
		return err
	}

	s.attachSession(conn, sess)
	return nil
}

func (s *Server) attachSession(conn net.Conn, sess *session.Session) {
	s.detachClient(conn)

	// Subscribe to session output
	outputCh := sess.Subscribe()

	state := &ClientState{
		session:  sess,
		outputCh: outputCh,
		stopCh:   make(chan struct{}),
	}

	s.mu.Lock()
	s.clients[conn] = state
	s.mu.Unlock()

	sess.Attach()

	// Start forwarding output to client
	go func() {
		for {
			select {
			case <-state.stopCh:
				return
			case data, ok := <-outputCh:
				if !ok {
					return
				}
				msg := protocol.NewMessage(protocol.MsgOutput, data)
				state.writeMu.Lock()
				err := protocol.Write(conn, msg)
				state.writeMu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	// Monitor window exits
	go func() {
		for {
			select {
			case <-state.stopCh:
				return
			case <-sess.WindowExitCh():
				// Check if all windows are closed
				if sess.WindowCount() == 0 {
					s.manager.Kill(sess.Name)
					s.checkAutoShutdown()
					s.detachClient(conn)
					resp := protocol.NewMessage(protocol.MsgDetachSession, nil)
					state.writeMu.Lock()
					protocol.Write(conn, resp)
					state.writeMu.Unlock()
					return
				}
			}
		}
	}()
}

func (s *Server) detachClient(conn net.Conn) {
	s.mu.Lock()
	state, ok := s.clients[conn]
	if ok {
		delete(s.clients, conn)
	}
	s.mu.Unlock()

	if ok && state != nil {
		close(state.stopCh)
		if state.session != nil {
			state.session.Detach()
			if state.outputCh != nil {
				state.session.Unsubscribe(state.outputCh)
			}
		}
	}
}

func (s *Server) handleDetachSession(conn net.Conn) error {
	s.detachClient(conn)
	resp := protocol.NewMessage(protocol.MsgSuccess, nil)
	return protocol.Write(conn, resp)
}

func (s *Server) handleListSessions(conn net.Conn) error {
	infos := s.manager.List()
	var data []byte
	for _, info := range infos {
		attached := ""
		if info.Attached {
			attached = " (attached)"
		}
		line := fmt.Sprintf("%s: %d windows (active: %d)%s\n",
			info.Name, info.WindowCount, info.ActiveWin, attached)
		data = append(data, []byte(line)...)
	}
	resp := protocol.NewMessage(protocol.MsgSuccess, data)
	return protocol.Write(conn, resp)
}

func (s *Server) handleKillSession(msg *protocol.Message) error {
	name := string(msg.Payload)
	err := s.manager.Kill(name)
	if err == nil {
		s.checkAutoShutdown()
	}
	return err
}

func (s *Server) handleInput(conn net.Conn, msg *protocol.Message) error {
	s.mu.Lock()
	state := s.clients[conn]
	s.mu.Unlock()

	if state == nil || state.session == nil {
		return nil
	}

	// Handle prefix key (Ctrl+B)
	data := msg.Payload
	regularStart := -1 // Track start of regular input batch

	flushRegular := func(end int) {
		if regularStart >= 0 && end > regularStart {
			state.session.Write(data[regularStart:end])
			regularStart = -1
		}
	}

	for i := 0; i < len(data); i++ {
		if state.prefixKey {
			state.prefixKey = false
			flushRegular(i) // Flush any pending regular input
			switch data[i] {
			case 'c': // New window
				state.session.NewWindow()
				continue
			case 'n': // Next window
				state.session.NextWindow()
				continue
			case 'p': // Previous window
				state.session.PrevWindow()
				continue
			case 'd': // Detach
				resp := protocol.NewMessage(protocol.MsgDetachSession, nil)
				state.writeMu.Lock()
				protocol.Write(conn, resp)
				state.writeMu.Unlock()
				s.detachClient(conn)
				return nil
			case '&': // Kill window
				state.session.KillWindow()
				if state.session.WindowCount() == 0 {
					s.manager.Kill(state.session.Name)
					s.checkAutoShutdown()
					resp := protocol.NewMessage(protocol.MsgDetachSession, nil)
					state.writeMu.Lock()
					protocol.Write(conn, resp)
					state.writeMu.Unlock()
					s.detachClient(conn)
					return nil
				}
				continue
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				idx := int(data[i] - '0')
				state.session.SelectWindow(idx)
				continue
			case 2: // Ctrl+B again, send literal Ctrl+B
				state.session.Write([]byte{2})
				continue
			default:
				// Unknown command, ignore
				continue
			}
		}

		if data[i] == 2 { // Ctrl+B
			flushRegular(i) // Flush any pending regular input
			state.prefixKey = true
			continue
		}

		// Mark start of regular input batch
		if regularStart < 0 {
			regularStart = i
		}
	}

	// Flush remaining regular input
	flushRegular(len(data))

	return nil
}

func (s *Server) handleResize(conn net.Conn, msg *protocol.Message) error {
	s.mu.Lock()
	state := s.clients[conn]
	s.mu.Unlock()

	if state == nil || state.session == nil {
		return nil
	}

	rows, cols, err := protocol.DecodeResize(msg.Payload)
	if err != nil {
		return err
	}

	return state.session.Resize(rows, cols)
}

func (s *Server) handleNewWindow(conn net.Conn) error {
	s.mu.Lock()
	state := s.clients[conn]
	s.mu.Unlock()

	if state == nil || state.session == nil {
		return nil
	}

	_, err := state.session.NewWindow()
	if err != nil {
		return err
	}

	resp := protocol.NewMessage(protocol.MsgSuccess, nil)
	return s.writeToConn(conn, resp)
}

func (s *Server) handleSelectWindow(conn net.Conn, msg *protocol.Message) error {
	s.mu.Lock()
	state := s.clients[conn]
	s.mu.Unlock()

	if state == nil || state.session == nil {
		return nil
	}

	if len(msg.Payload) < 1 {
		return nil
	}

	idx := int(msg.Payload[0])
	return state.session.SelectWindow(idx)
}

func (s *Server) handleKillWindow(conn net.Conn) error {
	s.mu.Lock()
	state := s.clients[conn]
	s.mu.Unlock()

	if state == nil || state.session == nil {
		return nil
	}

	if err := state.session.KillWindow(); err != nil {
		return err
	}

	// If no more windows, kill the session
	if state.session.WindowCount() == 0 {
		s.manager.Kill(state.session.Name)
		s.checkAutoShutdown()
		resp := protocol.NewMessage(protocol.MsgDetachSession, nil)
		state.writeMu.Lock()
		protocol.Write(conn, resp)
		state.writeMu.Unlock()
		s.detachClient(conn)
		return nil
	}

	resp := protocol.NewMessage(protocol.MsgSuccess, nil)
	return s.writeToConn(conn, resp)
}
