package cli

import (
	"fmt"
	"github.com/spf13/cobra"
)

type options struct {
	addr string
	grpc bool
}

func Execute() error {
	return newRootCmd().Execute()
}

func newRootCmd() *cobra.Command {
	opts := &options{}

	root := &cobra.Command{
		Use:   "goqueue",
		Short: "GoQueue command line client",
		Long:  "GoQueue CLI publishes, consumes, and fetches messages over TCP or gRPC.",
	}

	root.PersistentFlags().StringVar(&opts.addr, "addr", "localhost:9090", "broker address")
	root.PersistentFlags().BoolVar(&opts.grpc, "grpc", false, "use gRPC transport")

	root.AddCommand(newPublishCmd(opts))
	root.AddCommand(newPublishBatchCmd(opts))
	root.AddCommand(newConsumeCmd(opts))
	root.AddCommand(newFetchCmd(opts))
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print CLI version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("goqueue dev")
		},
	})

	return root
}
