package raft

import (
	"fmt"
	"os"
)

// Debugging
const Debug = false

/*
func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
*/

func DPrintf(format string, a ...interface{}) {
	if !Debug {
		return
	}
	debugLogFile, err := os.OpenFile("debug.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Failed to open debug log file: %v\n", err)
		return
	}
	defer debugLogFile.Close()
	if debugLogFile != nil {
		fmt.Fprintf(debugLogFile, format+"\n", a...)
	}
}
