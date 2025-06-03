package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
)

func main() {
	var data []byte
	var err error
	if len(os.Args) == 1 {
		data, err = io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unquote; unable to read stdin: %+v\n", err)
			os.Exit(1)
		}
	} else if len(os.Args) == 2 {
		f, err := os.Open(os.Args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "unquote; unable to open source file: %+v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		data, err = io.ReadAll(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unquote; unable to read source file: %+v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Fprintln(os.Stderr, "unquote; please either pipe data to this program with | or provide a filename as an argument")
		os.Exit(1)
	}
	unquoted, err := strconv.Unquote(string(data))
	if err != nil {
		fmt.Fprintf(os.Stderr, "unquote; unable to unquote string: %+v\n", err)
		os.Exit(1)
	}
	fmt.Fprint(os.Stdout, unquoted)
}
