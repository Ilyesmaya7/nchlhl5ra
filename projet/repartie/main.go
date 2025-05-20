package main

import (
	"fmt"
	"os"
	"projet/repartie/master"
	"projet/repartie/worker"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s master|worker [input_file1 ...|master_addr]\n", os.Args[0])
		os.Exit(1)
	}

	mode := os.Args[1]
	switch mode {
	case "master":
		inputFiles := os.Args[2:]
		nReduce := 2
		k := 5
		err := master.RunDistributed(inputFiles, nReduce, k)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Distributed MapReduce failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Distributed MapReduce completed. Check mr-final.txt for top words.")
	case "worker":
		masterAddr := os.Args[2]
		w, err := worker.NewWorker(masterAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Worker failed to start: %v\n", err)
			os.Exit(1)
		}
		err = w.Run()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Worker failed: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown mode: %s\n", mode)
		os.Exit(1)
	}
}