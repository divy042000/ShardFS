package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"gfs-client/cmd"
)

func main() {
	fmt.Println("GFS Client - Enter commands (e.g., 'write /data/myfile.txt', 'exit' to quit):")
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()
		if input == "exit" {
			break
		}
		// Simulate command-line args
		args := append([]string{"gfs-client"}, strings.Fields(input)...)
		os.Args = args
		cmd.Execute()
	}
}
