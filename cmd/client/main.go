package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "http://127.0.0.1:8000", "kvstored client API address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}

	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "put":
		err = doPut(*addr, rest)
	case "get":
		err = doGet(*addr, rest)
	case "delete":
		err = doDelete(*addr, rest)
	case "scan":
		err = doScan(*addr, rest)
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

func doPut(addr string, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: kvctl put <key> <value>")
	}
	key, value := args[0], args[1]
	req, err := http.NewRequest(http.MethodPut, addr+"/kv/"+url.PathEscape(key),
		strings.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put failed: %s: %s", resp.Status, body)
	}
	return nil
}

func doGet(addr string, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: kvctl get <key>")
	}
	resp, err := http.Get(addr + "/kv/" + url.PathEscape(args[0]))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		fmt.Println("not found")
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("get failed: %s: %s", resp.Status, body)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Println(string(body))
	return nil
}

func doDelete(addr string, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: kvctl delete <key>")
	}
	req, err := http.NewRequest(http.MethodDelete, addr+"/kv/"+url.PathEscape(args[0]), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed: %s: %s", resp.Status, body)
	}
	return nil
}

func doScan(addr string, args []string) error {
	prefix := ""
	if len(args) == 1 {
		prefix = args[0]
	} else if len(args) > 1 {
		return fmt.Errorf("usage: kvctl scan [prefix]")
	}
	resp, err := http.Get(addr + "/kv?prefix=" + url.QueryEscape(prefix))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("scan failed: %s: %s", resp.Status, body)
	}
	var results [][2]string
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return err
	}
	for _, kv := range results {
		fmt.Printf("%s=%s\n", kv[0], kv[1])
	}
	return nil
}

// 	var results [][2]string
// 	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
// 			return err
// 	}
// 	for _, kv := range results {
// 			fmt.Printf("%s=%s\n", kv[0], kv[1])
// 	}
// 	return nil
// }
