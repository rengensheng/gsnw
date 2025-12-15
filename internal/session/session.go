package session

import (
	"errors"
	"sync"
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrWindowNotFound  = errors.New("window not found")
)

// Session represents a terminal session with multiple windows
type Session struct {
	Name      string
	windows   []*Window
	activeWin int
	nextWinID int

	mu       sync.RWMutex
	attached bool
	closed   bool

	// Broadcast channel for output
	outputCh chan []byte
}

// NewSession creates a new session
func NewSession(name string) (*Session, error) {
	s := &Session{
		Name:      name,
		windows:   make([]*Window, 0),
		activeWin: 0,
		nextWinID: 0,
		outputCh:  make(chan []byte, 256),
	}

	// Create initial window
	if _, err := s.NewWindow(); err != nil {
		return nil, err
	}

	return s, nil
}

// NewWindow creates a new window in the session
func (s *Session) NewWindow() (*Window, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, err := NewWindow(s.nextWinID, "")
	if err != nil {
		return nil, err
	}

	s.windows = append(s.windows, w)
	s.nextWinID++

	// Start reading from the window
	go s.readWindow(w)

	return w, nil
}

// readWindow reads output from a window and broadcasts it
func (s *Session) readWindow(w *Window) {
	buf := make([]byte, 4096)
	for {
		n, err := w.Read(buf)
		if err != nil {
			return
		}
		if n > 0 {
			s.mu.RLock()
			if s.windows[s.activeWin] == w && s.attached {
				data := make([]byte, n)
				copy(data, buf[:n])
				select {
				case s.outputCh <- data:
				default:
					// Drop if buffer is full
				}
			}
			s.mu.RUnlock()
		}
	}
}

// ActiveWindow returns the currently active window
func (s *Session) ActiveWindow() *Window {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.windows) == 0 || s.activeWin >= len(s.windows) {
		return nil
	}
	return s.windows[s.activeWin]
}

// SelectWindow switches to a window by index
func (s *Session) SelectWindow(index int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 0 || index >= len(s.windows) {
		return ErrWindowNotFound
	}
	s.activeWin = index
	return nil
}

// NextWindow switches to the next window
func (s *Session) NextWindow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.windows) > 0 {
		s.activeWin = (s.activeWin + 1) % len(s.windows)
	}
}

// PrevWindow switches to the previous window
func (s *Session) PrevWindow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.windows) > 0 {
		s.activeWin = (s.activeWin - 1 + len(s.windows)) % len(s.windows)
	}
}

// KillWindow closes and removes the current window
func (s *Session) KillWindow() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.windows) == 0 {
		return ErrWindowNotFound
	}

	w := s.windows[s.activeWin]
	w.Close()

	// Remove from slice
	s.windows = append(s.windows[:s.activeWin], s.windows[s.activeWin+1:]...)

	// Adjust active window index
	if s.activeWin >= len(s.windows) && len(s.windows) > 0 {
		s.activeWin = len(s.windows) - 1
	}

	return nil
}

// WindowCount returns the number of windows
func (s *Session) WindowCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.windows)
}

// ActiveWindowIndex returns the active window index
func (s *Session) ActiveWindowIndex() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeWin
}

// Attach marks the session as attached
func (s *Session) Attach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attached = true
}

// Detach marks the session as detached
func (s *Session) Detach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attached = false
}

// IsAttached returns whether the session is attached
func (s *Session) IsAttached() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.attached
}

// Output returns the output channel
func (s *Session) Output() <-chan []byte {
	return s.outputCh
}

// Write sends input to the active window
func (s *Session) Write(data []byte) (int, error) {
	w := s.ActiveWindow()
	if w == nil {
		return 0, ErrWindowNotFound
	}
	return w.Write(data)
}

// Resize resizes the active window
func (s *Session) Resize(rows, cols uint16) error {
	w := s.ActiveWindow()
	if w == nil {
		return ErrWindowNotFound
	}
	return w.Resize(rows, cols)
}

// Close closes all windows and the session
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, w := range s.windows {
		w.Close()
	}
	close(s.outputCh)
}

// Manager manages multiple sessions
type Manager struct {
	sessions map[string]*Session
	mu       sync.RWMutex
}

// NewManager creates a new session manager
func NewManager() *Manager {
	return &Manager{
		sessions: make(map[string]*Session),
	}
}

// Create creates a new session
func (m *Manager) Create(name string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sessions[name]; exists {
		return nil, errors.New("session already exists")
	}

	s, err := NewSession(name)
	if err != nil {
		return nil, err
	}

	m.sessions[name] = s
	return s, nil
}

// Get returns a session by name
func (m *Manager) Get(name string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.sessions[name]
	if !ok {
		return nil, ErrSessionNotFound
	}
	return s, nil
}

// List returns all session names and info
func (m *Manager) List() []SessionInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	infos := make([]SessionInfo, 0, len(m.sessions))
	for name, s := range m.sessions {
		infos = append(infos, SessionInfo{
			Name:        name,
			WindowCount: s.WindowCount(),
			ActiveWin:   s.ActiveWindowIndex(),
			Attached:    s.IsAttached(),
		})
	}
	return infos
}

// Kill removes and closes a session
func (m *Manager) Kill(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.sessions[name]
	if !ok {
		return ErrSessionNotFound
	}

	s.Close()
	delete(m.sessions, name)
	return nil
}

// SessionInfo holds session metadata
type SessionInfo struct {
	Name        string
	WindowCount int
	ActiveWin   int
	Attached    bool
}
