package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"lsmdb/cluster"
)

func main() {
	addresses := flag.String("addresses", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "comma-separated cluster addresses")
	timeout := flag.Duration("timeout", 5*time.Second, "operation timeout")
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
	}
	client, err := cluster.NewClient(splitNonEmpty(*addresses))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch flag.Arg(0) {
	case "put":
		if flag.NArg() != 3 {
			usage()
		}
		response, err := client.Put(ctx, []byte(flag.Arg(1)), []byte(flag.Arg(2)))
		printJSON(response, err)
	case "delete":
		if flag.NArg() != 2 {
			usage()
		}
		response, err := client.Delete(ctx, []byte(flag.Arg(1)))
		printJSON(response, err)
	case "get":
		if flag.NArg() != 2 {
			usage()
		}
		response, err := client.Get(ctx, []byte(flag.Arg(1)))
		printJSON(response, err)
	case "status":
		response, err := client.Status(ctx)
		printJSON(response, err)
	default:
		usage()
	}
}

func splitNonEmpty(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func printJSON(value any, err error) {
	if err != nil {
		log.Fatal(err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(data))
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: lsmdbctl [flags] put KEY VALUE | get KEY | delete KEY | status")
	os.Exit(2)
}
