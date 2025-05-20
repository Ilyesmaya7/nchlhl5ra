# Distributed MapReduce System

## Overview

This project implements a distributed MapReduce system in Go, designed to count word frequencies across multiple input files. It follows the MapReduce paradigm, with a master coordinating map and reduce tasks executed by workers. The system includes fault tolerance for worker crashes (5% chance) and delays (10% chance, 0-5s). A web dashboard, accessible at http://localhost:8080, displays real-time task status, worker assignments, and job progress. The final output is written to `mr-final.txt`, listing the top 5 words by frequency.

This project was developed for the PDIST25 course by \[Student1 Name\] (ID: ETUDIANT1) and \[Student2 Name\] (ID: ETUDIANT2).

## Directory Structure

```
PDIST25-ETUDIANT1-ETUDIANT2/
├── common/
│   └── types.go        # Data structures (Task, TaskType)
├── mapreduce/
│   └── mapreduce.go    # Map and reduce functions for word counting
├── master/
│   └── master.go       # Master logic, task coordination, HTTP server
├── worker/
│   └── worker.go       # Worker logic, task execution
├── web/
│   ├── index.html      # Dashboard UI (HTML, Tailwind CSS, Chart.js)
│   └── script.js       # Dashboard JavaScript (fetches /data endpoint)
├── main.go             # Entry point, launches master or worker
├── input1.txt          # Sample input: "hello world hello this is a test"
├── input2.txt          # Sample input: "world goodbye hello test test world"
└── README.md           # This file
```

## Setup Instructions

### Prerequisites

- Install Go (version 1.16 or later): golang.org
- Ensure a modern web browser (e.g., Chrome, Firefox) for the dashboard.

### Clone the Project

1. Copy the project directory `PDIST25-ETUDIANT1-ETUDIANT2/` to your machine.

2. Alternatively, initialize as a Git repository (optional):

   ```bash
   git clone <repository-url>
   cd PDIST25-ETUDIANT1-ETUDIANT2
   ```

### Verify Files

- Ensure `input1.txt` and `input2.txt` exist in the root directory.
- Confirm `web/index.html` and `web/script.js` are in the `web/` directory.

## Usage

The system consists of a master, multiple workers, and a web dashboard.

### Clean Up Old Files

Remove previous intermediate and output files:

```bash
rm -f mr-* mr-out-* mr-final.txt
```

### Run the Master

Start the master with input files (e.g., `input1.txt`, `input2.txt`):

```bash
go run main.go master input1.txt input2.txt
```

The master listens on `:1234` (RPC) and `:8080` (HTTP dashboard). It coordinates tasks and writes the top 5 words to `mr-final.txt`.

### Run Workers

In separate terminals, start 2-3 workers:

```bash
go run main.go worker localhost:1234
```

Workers execute map and reduce tasks, with a 5% chance of crashing and 10% chance of delay (0-5s).

### View the Dashboard

Open http://localhost:8080 in a browser. The dashboard displays:

- **Task Table**: Task ID, type (map/reduce), status (idle/in-progress/done).
- **Worker Table**: Worker ID, number of tasks assigned.
- **Progress Bar**: Job completion percentage (0-100%).

Updates every second via the `/data` endpoint.

### Check Output

After completion, view `mr-final.txt`:

```bash
cat mr-final.txt
```

Expected output for sample inputs:

```
hello: 3
test: 3
world: 3
a: 1
goodbye: 1
```

### Stop the System

Press `Ctrl+C` in the master terminal to shut down (workers exit automatically).
