package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd is the base command called without any subcommands.
var rootCmd = &cobra.Command{
	Use:   "ha-tools",
	Short: "CLI utilities for Home Assistant workflows",
	Long: `ha-tools bundles helpful commands for interacting with Home Assistant
and related automation tooling.`,
}

// Execute runs the root command and propagates any failure to os.Exit.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
