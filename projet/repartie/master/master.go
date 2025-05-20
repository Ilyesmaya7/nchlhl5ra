package master

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"projet/repartie/common"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"net/http"
	"encoding/json"
	"os/signal"
	"syscall"
	// "/mapreduce"
)




// Master manages the MapReduce job
type Master struct {
	mu          sync.Mutex
	tasks       []common.Task
	taskStates  map[int]string // "idle", "in-progress", "done"
	taskTimes   map[int]time.Time
	nReduce     int
	inputFiles  []string
	done        bool
	server      *rpc.Server
	workers     map[string]int // worker ID -> number of tasks assigned
	workerCount int
}

// NewMaster initializes a new Master
func NewMaster(inputFiles []string, nReduce int) *Master {
	m := &Master{
		tasks:       make([]common.Task, 0),
		taskStates:  make(map[int]string),
		taskTimes:   make(map[int]time.Time),
		nReduce:     nReduce,
		inputFiles:  inputFiles,
		workers:     make(map[string]int),
		workerCount: 0,
	}
	for i, file := range inputFiles {
		task := common.Task{
			Type:            common.MapTask,
			ID:              i,
			JobName:         "wordcount",
			File:            file,
			NReduce:         nReduce,
			NMap:            len(inputFiles),
			ReduceTaskNumber: 0,
		}
		m.tasks = append(m.tasks, task)
		m.taskStates[i] = "idle"
	}
	for i := 0; i < nReduce; i++ {
		task := common.Task{
			Type:            common.ReduceTask,
			ID:              len(inputFiles) + i,
			JobName:         "wordcount",
			NReduce:         nReduce,
			NMap:            len(inputFiles),
			ReduceTaskNumber: i,
		}
		m.tasks = append(m.tasks, task)
		m.taskStates[len(inputFiles)+i] = "idle"
	}
	return m
}

// GetTask assigns a task to a worker
func (m *Master) GetTask(args *common.GetTaskArgs, reply *common.GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		reply.Task = common.Task{Type: common.DoneTask}
		return nil
	}

	workerID := fmt.Sprintf("worker-%d", m.workerCount)
	m.workers[workerID]++

	mapDone := true
	for i := 0; i < len(m.inputFiles); i++ {
		if m.taskStates[i] != "done" {
			mapDone = false
			break
		}
	}

	now := time.Now()
	for _, task := range m.tasks {
		if task.Type == common.ReduceTask && !mapDone {
			continue
		}
		if m.taskStates[task.ID] == "idle" {
			m.taskStates[task.ID] = "in-progress"
			m.taskTimes[task.ID] = now
			reply.Task = task
			fmt.Printf("Assigned task %d (%s) to %s\n", task.ID, task.Type, workerID)
			return nil
		}
		if m.taskStates[task.ID] == "in-progress" && now.Sub(m.taskTimes[task.ID]) > 10*time.Second {
			m.taskStates[task.ID] = "in-progress"
			m.taskTimes[task.ID] = now
			reply.Task = task
			fmt.Printf("Reassigning task %d (%s) to %s\n", task.ID, task.Type, workerID)
			return nil
		}
	}

	allDone := true
	for _, state := range m.taskStates {
		if state != "done" {
			allDone = false
			break
		}
	}
	if allDone {
		m.done = true
		reply.Task = common.Task{Type: common.DoneTask}
	} else {
		reply.Task = common.Task{Type: common.WaitTask}
	}
	return nil
}

// ReportTaskDone marks a task as completed
func (m *Master) ReportTaskDone(args *common.ReportTaskDoneArgs, reply *common.ReportTaskDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.taskStates[args.TaskID] == "in-progress" && m.tasks[args.TaskID].Type == args.Type {
		m.taskStates[args.TaskID] = "done"
		reply.Success = true
		fmt.Printf("Task %d (%s) completed\n", args.TaskID, args.Type)
	} else {
		reply.Success = false
	}
	return nil
}

// StartRPC starts the RPC server
func (m *Master) StartRPC() error {
	m.server = rpc.NewServer()
	if err := m.server.Register(m); err != nil {
		return fmt.Errorf("failed to register RPC: %v", err)
	}
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Accept error: %v", err)
				continue
			}
			m.server.ServeConn(conn)
		}
	}()
	return nil
}

// StartHTTP starts the HTTP server for the dashboard
func (m *Master) StartHTTP() error {
	// Serve index.html for root
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			log.Printf("404: Path %s not found", r.URL.Path)
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}
		log.Printf("Serving index.html for %s", r.URL.Path)
		http.ServeFile(w, r, "web/index.html")
	})

	// Serve script.js explicitly
	http.HandleFunc("/script.js", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Serving script.js")
		http.ServeFile(w, r, "web/script.js")
	})

	// Serve /data endpoint
	http.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()

		data := struct {
			Workers []struct {
				ID            string `json:"id"`
				TasksAssigned int    `json:"tasks_assigned"`
			} `json:"workers"`
			Tasks []struct {
				ID     int    `json:"id"`
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"tasks"`
			Progress float64 `json:"progress"`
		}{}

		for id, tasks := range m.workers {
			data.Workers = append(data.Workers, struct {
				ID            string `json:"id"`
				TasksAssigned int    `json:"tasks_assigned"`
			}{ID: id, TasksAssigned: tasks})
		}

		for _, task := range m.tasks {
			data.Tasks = append(data.Tasks, struct {
				ID     int    `json:"id"`
				Type   string `json:"type"`
				Status string `json:"status"`
			}{ID: task.ID, Type: string(task.Type), Status: m.taskStates[task.ID]})
		}

		totalTasks := len(m.tasks)
		doneTasks := 0
		for _, state := range m.taskStates {
			if state == "done" {
				doneTasks++
			}
		}
		if totalTasks > 0 {
			data.Progress = float64(doneTasks) / float64(totalTasks) * 100
		}

		log.Printf("Serving /data: %d tasks, %d workers, %.2f%% progress", len(data.Tasks), len(data.Workers), data.Progress)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")
		if err := json.NewEncoder(w).Encode(data); err != nil {
			log.Printf("Error encoding /data: %v", err)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"error": "Internal server error"})
			return
		}
	})

	go func() {
		log.Printf("Starting HTTP server on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()
	return nil
}

// mergeOutputs combines reduce outputs and prints the top k words
func mergeOutputs(jobName string, nReduce int, k int) error {
	wordCounts := make(map[string]int)
	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-out-%d", i)
		content, err := os.ReadFile(fileName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mergeOutputs: failed to read %s: %v\n", fileName, err)
			continue
		}
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			parts := strings.Fields(line)
			if len(parts) != 2 {
				fmt.Fprintf(os.Stderr, "mergeOutputs: invalid line in %s: %s\n", fileName, line)
				continue
			}
			word, countStr := parts[0], parts[1]
			count, err := strconv.Atoi(countStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "mergeOutputs: invalid count in %s: %s\n", fileName, countStr)
				continue
			}
			wordCounts[word] += count
		}
	}
	type wordCount struct {
		Word  string
		Count int
	}
	var sortedCounts []wordCount
	for word, count := range wordCounts {
		sortedCounts = append(sortedCounts, wordCount{word, count})
	}
	sort.Slice(sortedCounts, func(i, j int) bool {
		if sortedCounts[i].Count == sortedCounts[j].Count {
			return sortedCounts[i].Word < sortedCounts[j].Word
		}
		return sortedCounts[i].Count > sortedCounts[j].Count
	})
	outputFile, err := os.Create("mr-final.txt")
	if err != nil {
		return fmt.Errorf("mergeOutputs: failed to create mr-final.txt: %v", err)
	}
	defer outputFile.Close()
	for i, wc := range sortedCounts {
		if i >= k {
			break
		}
		fmt.Fprintf(outputFile, "%s: %d\n", wc.Word, wc.Count)
		fmt.Printf("Top %d: %s: %d\n", i+1, wc.Word, wc.Count)
	}
	return nil
}

// RunDistributed runs the distributed MapReduce job
func RunDistributed(inputFiles []string, nReduce int, k int) error {
	m := NewMaster(inputFiles, nReduce)
	if err := m.StartRPC(); err != nil {
		return err
	}
	if err := m.StartHTTP(); err != nil {
		return err
	}
	for {
		m.mu.Lock()
		if m.done {
			m.mu.Unlock()
			break
		}
		m.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
	if err := mergeOutputs("wordcount", nReduce, k); err != nil {
		return err
	}
	log.Println("Job completed. HTTP server running on :8080. Press Ctrl+C to exit.")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("Shutting down")
	return nil
}