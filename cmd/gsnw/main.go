package main

import (
	"fmt"
	"os"
	"time"

	"github.com/rengensheng/gsnw/internal/client"
	"github.com/rengensheng/gsnw/internal/server"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "gsnw",
	Short: "A terminal multiplexer similar to tmux",
	Long:  "gsnw is a terminal multiplexer that allows you to create and manage multiple terminal sessions.",
	Run: func(cmd *cobra.Command, args []string) {
		// Default behavior: attach to existing session or create new one
		ensureServer()

		c, err := client.Connect()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		// Try to list sessions first
		list, err := c.List()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		// Reconnect after list
		c.Close()
		c, err = client.Connect()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		if list == "" {
			// No sessions, create new one
			if err := c.NewSession(""); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		} else {
			// Attach to first session
			// Parse first session name
			var firstName string
			for i, ch := range list {
				if ch == ':' {
					firstName = list[:i]
					break
				}
			}
			if firstName != "" {
				if err := c.Attach(firstName); err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					os.Exit(1)
				}
			}
		}
	},
}

var newCmd = &cobra.Command{
	Use:   "new",
	Short: "Create a new session",
	Run: func(cmd *cobra.Command, args []string) {
		name, _ := cmd.Flags().GetString("session")
		ensureServer()

		c, err := client.Connect()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		if err := c.NewSession(name); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	},
}

var attachCmd = &cobra.Command{
	Use:   "attach",
	Short: "Attach to an existing session",
	Run: func(cmd *cobra.Command, args []string) {
		name, _ := cmd.Flags().GetString("target")
		if name == "" {
			fmt.Fprintf(os.Stderr, "Error: session name required (-t)\n")
			os.Exit(1)
		}

		ensureServer()

		c, err := client.Connect()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		if err := c.Attach(name); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	},
}

var listCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List all sessions",
	Run: func(cmd *cobra.Command, args []string) {
		if !client.IsServerRunning() {
			fmt.Println("No sessions.")
			return
		}

		c, err := client.Connect()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		list, err := c.List()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if list == "" {
			fmt.Println("No sessions.")
		} else {
			fmt.Print(list)
		}
	},
}

var killCmd = &cobra.Command{
	Use:   "kill",
	Short: "Kill a session",
	Run: func(cmd *cobra.Command, args []string) {
		name, _ := cmd.Flags().GetString("target")
		if name == "" {
			fmt.Fprintf(os.Stderr, "Error: session name required (-t)\n")
			os.Exit(1)
		}

		if !client.IsServerRunning() {
			fmt.Fprintf(os.Stderr, "Error: server not running\n")
			os.Exit(1)
		}

		c, err := client.Connect()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		if err := c.Kill(name); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Session '%s' killed.\n", name)
	},
}

var serverCmd = &cobra.Command{
	Use:    "server",
	Short:  "Start the server (internal use)",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		s := server.New()
		if err := s.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	newCmd.Flags().StringP("session", "s", "", "Session name")
	attachCmd.Flags().StringP("target", "t", "", "Target session name")
	killCmd.Flags().StringP("target", "t", "", "Target session name")

	rootCmd.AddCommand(newCmd)
	rootCmd.AddCommand(attachCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(killCmd)
	rootCmd.AddCommand(serverCmd)
}

func ensureServer() {
	if !client.IsServerRunning() {
		if err := client.StartServer(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to start server: %v\n", err)
			os.Exit(1)
		}
		// Wait for server to start
		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			if client.IsServerRunning() {
				return
			}
		}
		fmt.Fprintf(os.Stderr, "Server failed to start\n")
		os.Exit(1)
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
