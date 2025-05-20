package worker

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"time"
	"projet/repartie/common"
	"projet/repartie/mapreduce"
)


// Worker represents a MapReduce worker
type Worker struct {
	masterAddr string
	client     *rpc.Client
}

// NewWorker initializes a new Worker
func NewWorker(masterAddr string) (*Worker, error) {
	client, err := rpc.Dial("tcp", masterAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %v", err)
	}
	return &Worker{
		masterAddr: masterAddr,
		client:     client,
	}, nil
}

// Run starts the worker
func (w *Worker) Run() error {
	for {
		// Simulate crash (5% chance)
		if rand.Float64() < 0.05 {
			fmt.Printf("Worker crashed\n")
			os.Exit(1)
		}
		// Simulate delay (10% chance, 0-5 seconds)
		if rand.Float64() < 0.10 {
			delay := time.Duration(rand.Intn(5)) * time.Second
			fmt.Printf("Worker delayed for %v\n", delay)
			time.Sleep(delay)
		}

		// Request a task
		var reply common.GetTaskReply
		err := w.client.Call("Master.GetTask", &common.GetTaskArgs{}, &reply)
		if err != nil {
			fmt.Printf("GetTask failed: %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}

		switch reply.Task.Type {
		case common.MapTask:
			fmt.Printf("Worker executing map task %d for file %s\n", reply.Task.ID, reply.Task.File)
			mymapreduce.DoMap(reply.Task.JobName, reply.Task.ID, reply.Task.File, reply.Task.NReduce, mymapreduce.MapF)
			w.reportTaskDone(reply.Task.ID, common.MapTask)
		case common.ReduceTask:
			fmt.Printf("Worker executing reduce task %d (index %d)\n", reply.Task.ID, reply.Task.ReduceTaskNumber)
			mymapreduce.DoReduce(reply.Task.JobName, reply.Task.ReduceTaskNumber, reply.Task.NMap, mymapreduce.ReduceF)
			w.reportTaskDone(reply.Task.ID, common.ReduceTask)
		case common.WaitTask:
			time.Sleep(1 * time.Second)
		case common.DoneTask:
			fmt.Printf("Worker received done signal\n")
			return nil
		}
	}
}

// reportTaskDone notifies the master of task completion
func (w *Worker) reportTaskDone(taskID int, taskType common.TaskType) {
	args := &common.ReportTaskDoneArgs{TaskID: taskID, Type: taskType}
	var reply common.ReportTaskDoneReply
	err := w.client.Call("Master.ReportTaskDone", args, &reply)
	if err != nil || !reply.Success {
		fmt.Printf("ReportTaskDone failed for task %d: %v\n", taskID, err)
	}
}