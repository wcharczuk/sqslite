package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"github.com/wcharczuk/sqslite/internal/spy"
	"github.com/wcharczuk/sqslite/internal/sqslite"
)

var (
	flagTarget = pflag.String("target", "AmazonSQS.ReceiveMessage", "Specific targets to print")
	flagLimit  = pflag.Int("limit", 1, "The number to print in total")
)

func main() {
	pflag.Parse()

	if len(pflag.Args()) != 1 {
		fmt.Fprintln(os.Stderr, "read-response-bodies; provide a filename as an argument")
		os.Exit(1)
	}

	f, err := os.Open(pflag.Args()[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "read-response-bodies; unable to open source file: %+v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	jsonEncoder := json.NewEncoder(os.Stdout)
	jsonEncoder.SetIndent("", "  ")

	var count int
	for scanner.Scan() {
		line := scanner.Text()
		var req spy.Request
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			fmt.Fprintf(os.Stderr, "read-response-bodies; unable to deserialize source file: %+v\n", err)
			os.Exit(1)
		}
		if *flagTarget != "" {
			header, ok := req.RequestHeaders[sqslite.HeaderAmzTarget]
			if !ok {
				continue
			}
			if header != *flagTarget {
				continue
			}
		}
		var responseBodyDeserialized any
		if err := json.Unmarshal([]byte(req.ResponseBody), &responseBodyDeserialized); err != nil {
			fmt.Fprintf(os.Stderr, "read-response-bodies; unable to deserialize response body: %+v\n", err)
			os.Exit(1)
		}
		if err := jsonEncoder.Encode(responseBodyDeserialized); err != nil {
			fmt.Fprintf(os.Stderr, "read-response-bodies; unable to re-serialize response body: %+v\n", err)
			os.Exit(1)
		}
		count++
		if *flagLimit > 0 && count > *flagLimit {
			break
		}
	}

}
