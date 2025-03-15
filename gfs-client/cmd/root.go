package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "gfs-client",
	Short: "A client for the GFS distributed file system",
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	// Persistent Flags for master server address
	rootCmd.PersistentFlags().StringP("master", "m", "localhost:50052", "Master server gRPC address")
}