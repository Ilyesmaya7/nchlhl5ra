package common

// KeyValue represents a key-value pair for MapReduce
type KeyValue struct {
	Key   string
	Value string
}

// TaskType defines the type of task
type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	WaitTask   TaskType = "wait"
	DoneTask   TaskType = "done"
)

// Task represents a MapReduce task
type Task struct {
	Type            TaskType
	ID              int
	JobName         string
	File            string // For map tasks: input file; for reduce tasks: not used
	NReduce         int    // Number of reduce tasks
	NMap            int    // Number of map tasks (for reduce tasks)
	ReduceTaskNumber int    // For reduce tasks: index (0 to NReduce-1)
}

// GetTaskArgs for RPC call
type GetTaskArgs struct{}

// GetTaskReply for RPC response
type GetTaskReply struct {
	Task Task
}

// ReportTaskDoneArgs for RPC call
type ReportTaskDoneArgs struct {
	TaskID int
	Type   TaskType
}

// ReportTaskDoneReply for RPC response
type ReportTaskDoneReply struct {
	Success bool
}