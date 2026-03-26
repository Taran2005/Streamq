// streamq-cli is the admin CLI for StreamQ.
//
// Usage:
//   streamq-cli <command> <subcommand> [flags]
//
// Commands:
//   topic create | list | describe
//   partitions list | reassign
//   produce
//   consume
//
// Most subcommands talk to a broker over its HTTP API. The `partitions`
// subcommand talks to etcd directly so we can inspect/edit assignments
// even when no broker is reachable.
package main

import (
	"fmt"
	"os"
)

const usage = `streamq-cli — admin tool for StreamQ

Usage:
  streamq-cli <command> [subcommand] [flags]

Commands:
  topic create      Create a topic (HTTP)
  topic list        List topics (HTTP)
  topic describe    Show partition->broker map for a topic (HTTP /metadata)
  partitions list   List all partition assignments from etcd
  partitions reassign  Reassign a partition to a different broker (etcd write)
  produce           Send a single message (HTTP)
  consume           Read messages from a partition (HTTP)

Run "streamq-cli <command> --help" for flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	switch os.Args[1] {
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	case "topic":
		runTopic(os.Args[2:])
	case "partitions":
		runPartitions(os.Args[2:])
	case "produce":
		runProduce(os.Args[2:])
	case "consume":
		runConsume(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
}

// dieIf prints err to stderr and exits with code 1 if err is non-nil.
// Used everywhere — keeps the subcommand handlers from drowning in
// `if err != nil { ... }` boilerplate.
func dieIf(err error, prefix string) {
	if err == nil {
		return
	}
	if prefix != "" {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prefix, err)
	} else {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
	os.Exit(1)
}
