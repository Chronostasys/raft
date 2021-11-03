package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/Chronostasys/raft"
	"github.com/Chronostasys/raft/kvraft"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
)

func main() {
	ends := os.Args[1:]
	if len(ends) == 0 {
		println("kvraft cli")
		println("Usage:")
		println("	$cli [endpoint1] [endpoint2] [endpoint3]...")
		println("Example:")
		println("	$cli :1234 :1235 :1236")
		os.Exit(1)
	}
	rpcends := raft.MakeRPCEnds(ends)
	client := kvraft.MakeClerk(rpcends)
	writer := bufio.NewWriter(os.Stdout)
	reader := bufio.NewReader(os.Stdin)
	for {
		_, err := writer.WriteString("cli> ")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		err = writer.Flush()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		// Read the keyboad input.
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		input = strings.Trim(input, "\n\r ")
		if len(strings.Trim(input, " ")) == 0 {
			continue
		}
		if input == "help" {
			printHelp()
			continue
		}
		cc := strings.Split(input, "\"")
		cmds := []string{}
		for i, v := range cc {
			if i%2 == 0 {
				v = strings.Trim(v, " ")
				if len(v) == 0 {
					continue
				}
				cmds = append(cmds, strings.Split(v, " ")...)
			} else {
				cmds = append(cmds, v)
			}
		}
		if cmds[0] == "exit" {
			return
		}
		if cmds[0] == "append" {
			if len(cmds) != 3 {
				println(colorRed, "invalid input", input, colorReset)
				printHelp()
				continue
			}
			client.Append(strings.Trim(cmds[1], "\""), strings.Trim(cmds[2], "\""))
		} else if cmds[0] == "put" {
			if len(cmds) != 3 {
				println(colorRed, "invalid input", input, colorReset)
				printHelp()
				continue
			}
			client.Put(strings.Trim(cmds[1], "\""), strings.Trim(cmds[2], "\""))
		} else if cmds[0] == "get" {
			if len(cmds) != 2 {
				println(colorRed, "invalid input", input, colorReset)
				printHelp()
				continue
			}
			v := client.Get(strings.Trim(cmds[1], "\""))
			println("Key", colorGreen, "\""+cmds[1]+"\"", colorReset, "Val", colorBlue, "\""+v+"\"", colorReset)
		} else {
			println(colorRed, "Unknown command:", cmds[0], colorReset)
			printHelp()
		}
	}
}

func printHelp() {
	println(colorYellow)
	println("---------------help-----------------")
	println("append [key] [value] - append value")
	println("put [key] [value]    - update value")
	println("get [key]            - get value")
	println("exit                 - exit")
	println("Examples:")
	println("	put a b")
	println("	append a \"b c\"")
	println(colorReset)
}
