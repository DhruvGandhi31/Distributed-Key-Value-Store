package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/client"
)

func main() {
	addr := flag.String("addr", "http://127.0.0.1:8000", "kvstored client API address(es); comma-separated to give the SDK more than one node to try if one is down or not the leader")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}

	endpoints := parseEndpoints(*addr)
	c, err := client.New(client.Config{Endpoints: endpoints})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "put":
		err = doPut(ctx, c, rest)
	case "get":
		err = doGet(ctx, c, rest)
	case "delete":
		err = doDelete(ctx, c, rest)
	case "scan":
		err = doScan(ctx, c, rest)
	default:
		usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
kvctl put <key> <value>
kvctl get <key>
kvctl delete <key>
kvctl scan [prefix]`)
}

func parseEndpoints(addr string) []string {
	var endpoints []string
	for _, e := range strings.Split(addr, ",") {
		e = strings.TrimSpace(e)
		if e != "" {
			endpoints = append(endpoints, e)
		}
	}
	return endpoints
}

func doPut(ctx context.Context, c *client.Client, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: kvctl put <key> <value>")
	}
	return c.Put(ctx, args[0], []byte(args[1]))
}

func doGet(ctx context.Context, c *client.Client, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: kvctl get <key>")
	}
	value, ok, err := c.Get(ctx, args[0])
	if err != nil {
		return err
	}
	if !ok {
		fmt.Println("not found")
		return nil
	}
	fmt.Println(string(value))
	return nil
}

func doDelete(ctx context.Context, c *client.Client, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: kvctl delete <key>")
	}
	return c.Delete(ctx, args[0])
}

func doScan(ctx context.Context, c *client.Client, args []string) error {
	prefix := ""
	if len(args) == 1 {
		prefix = args[0]
	} else if len(args) > 1 {
		return fmt.Errorf("usage: kvctl scan [prefix]")
	}
	results, err := c.Scan(ctx, prefix)
	if err != nil {
		return err
	}
	for _, kv := range results {
		fmt.Printf("%s=%s\n", kv[0], kv[1])
	}
	return nil
}
