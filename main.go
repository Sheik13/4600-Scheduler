package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// go run main.go example_processes.csv

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	//store the original burst times, and use this to keep track of how long each process has left
	procTimes := []int64{int64(processes[0].BurstDuration)}

	for y := 1; y < len(processes); y++ {
		procTimes = append(procTimes, processes[y].BurstDuration)
	}

	var totalLoopIterations = 0

	var curProcess = 0
	var shortestTime = 999999
	var highestPriority = 999999

	//keeps track of how much time we've spent on the current process
	var curBurst = 0

	//figure out how many times to run the loop based on how long each process needs
	for z := range processes {
		totalLoopIterations += int(processes[z].BurstDuration)
	}

	for i := 0; i <= totalLoopIterations; i++ {
		serviceTime = int64(i)
		//set shortest time to a very high number
		shortestTime = 999999
		highestPriority = 999999
		//keep track of what process we had last loop
		prevProc := curProcess

		//find the process with the higest priority left to do
		//if we find ones with the same priority, choose the shorter one
		for j := range processes {
			if processes[j].Priority < int64(highestPriority) && processes[j].ArrivalTime <= int64(i) && procTimes[j] > 0 {
				highestPriority = int(processes[j].Priority)
				curProcess = j
				shortestTime = int(procTimes[j])
			}
			if processes[j].Priority == int64(highestPriority) && processes[j].ArrivalTime <= int64(i) && procTimes[j] > 0 && procTimes[j] < int64(shortestTime) {
				highestPriority = int(processes[j].Priority)
				curProcess = j
				shortestTime = int(procTimes[j])
			}
		}

		//reset the current burst if we change process
		if prevProc != curProcess {
			curBurst = 0
		}

		//take one away from the current process' time
		//add one to the current burst streak
		procTimes[curProcess] -= 1
		curBurst += 1

		//if a process finishes or a process is premepted
		if procTimes[curProcess] == 0 || (i > 0 && prevProc != curProcess && procTimes[prevProc] != 0) {

			//if the process was preempted, add the preempted process to the gantt
			if i > 0 && prevProc != curProcess && procTimes[prevProc] != 0 && procTimes[curProcess] != 0 {
				waitingTime = serviceTime - (processes[prevProc].ArrivalTime + (processes[prevProc].BurstDuration - procTimes[prevProc]))

				start := waitingTime + processes[prevProc].ArrivalTime

				gantt = append(gantt, TimeSlice{
					PID:   processes[prevProc].ProcessID,
					Start: start,
					Stop:  serviceTime + 1,
				})
			}

			//if the process finished, add it to the gantt chart, and add it to the schedule
			if procTimes[curProcess] == 0 {

				waitingTime = serviceTime - (processes[curProcess].ArrivalTime + int64(curBurst)) + 1

				start := waitingTime + processes[curProcess].ArrivalTime

				//waitingTime = serviceTime - (processes[curProcess].ArrivalTime + processes[curProcess].BurstDuration) + 1
				totalWait += float64(waitingTime)

				turnaround := serviceTime + 1 - processes[curProcess].ArrivalTime
				totalTurnaround += float64(turnaround)

				completion := serviceTime + 1
				lastCompletion = float64(completion)

				schedule[curProcess] = []string{
					fmt.Sprint(processes[curProcess].ProcessID),
					fmt.Sprint(processes[curProcess].Priority),
					fmt.Sprint(processes[curProcess].BurstDuration),
					fmt.Sprint(processes[curProcess].ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}

				gantt = append(gantt, TimeSlice{
					PID:   processes[curProcess].ProcessID,
					Start: start,
					Stop:  serviceTime + 1,
				})

			}
		}

	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

}

// go run main.go example_processes.csv
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	//store the original burst times, and use this to keep track of how long each process has left
	procTimes := []int64{int64(processes[0].BurstDuration)}

	for y := 1; y < len(processes); y++ {
		procTimes = append(procTimes, processes[y].BurstDuration)
	}

	var totalLoopIterations = 0

	var curProcess = 0
	var shortestTime = 999999

	//keeps track of how much time we've spent on the current process
	var curBurst = 0

	//figure out how many times to run the loop based on how long each process needs
	for z := range processes {
		totalLoopIterations += int(processes[z].BurstDuration)
	}

	for i := 0; i <= totalLoopIterations; i++ {
		serviceTime = int64(i)
		//set shortest time to a very high number
		shortestTime = 999999
		//keep track of what process we had last loop
		prevProc := curProcess

		//find the process with the shortest burst left to do, that has reached the scheduler
		for j := range processes {
			if processes[j].ArrivalTime <= int64(i) && procTimes[j] > 0 && procTimes[j] < int64(shortestTime) {
				curProcess = j
				shortestTime = int(procTimes[j])
			}
		}

		//reset the current burst if we change process
		if prevProc != curProcess {
			curBurst = 0
		}

		//take one away from the current process' time
		//add one to the current burst streak
		procTimes[curProcess] -= 1
		curBurst += 1

		//if a process finishes or a process is premepted
		if procTimes[curProcess] == 0 || (i > 0 && prevProc != curProcess && procTimes[prevProc] != 0) {

			//if the process was preempted, add the preempted process to the gantt
			if i > 0 && prevProc != curProcess && procTimes[prevProc] != 0 && procTimes[curProcess] != 0 {
				waitingTime = serviceTime - (processes[prevProc].ArrivalTime + (processes[prevProc].BurstDuration - procTimes[prevProc]))

				start := waitingTime + processes[prevProc].ArrivalTime

				gantt = append(gantt, TimeSlice{
					PID:   processes[prevProc].ProcessID,
					Start: start,
					Stop:  serviceTime + 1,
				})
			}

			//if the process finished, add it to the gantt chart, and add it to the schedule
			if procTimes[curProcess] == 0 {

				waitingTime = serviceTime - (processes[curProcess].ArrivalTime + int64(curBurst)) + 1

				start := waitingTime + processes[curProcess].ArrivalTime

				//waitingTime = serviceTime - (processes[curProcess].ArrivalTime + processes[curProcess].BurstDuration) + 1
				totalWait += float64(waitingTime)

				turnaround := serviceTime + 1 - processes[curProcess].ArrivalTime
				totalTurnaround += float64(turnaround)

				completion := serviceTime + 1
				lastCompletion = float64(completion)

				schedule[curProcess] = []string{
					fmt.Sprint(processes[curProcess].ProcessID),
					fmt.Sprint(processes[curProcess].Priority),
					fmt.Sprint(processes[curProcess].BurstDuration),
					fmt.Sprint(processes[curProcess].ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}

				gantt = append(gantt, TimeSlice{
					PID:   processes[curProcess].ProcessID,
					Start: start,
					Stop:  serviceTime + 1,
				})

			}

		}

	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// go run main.go example_processes.csv
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	//timeslice
	const TIMESLICE = 4

	//store the original burst times, and use this to keep track of how long each process has left
	procTimes := []int64{int64(processes[0].BurstDuration)}

	for y := 1; y < len(processes); y++ {
		procTimes = append(procTimes, processes[y].BurstDuration)
	}

	var totalLoopIterations = 0

	var curProcess = 0

	//keeps track of how much time we've spent on the current process
	var curBurst = 0

	//figure out how many times to run the loop based on how long each process needs
	for z := range processes {
		totalLoopIterations += int(processes[z].BurstDuration)
	}

	for i := 0; i < totalLoopIterations; i++ {
		serviceTime = int64(i)

		//keep track of what process we had last loop
		prevProc := curProcess

		//if a process finishes or the timeslice is over, switch
		if curBurst == TIMESLICE || procTimes[curProcess] == 0 {
			curProcess += 1
			curBurst = 0
			//if we were at the last process, change to the first process
			if curProcess >= len(processes) {
				curProcess = 0
			}

			//make sure the process hasn't finished, if it has, keep going till we find the next one
			for procTimes[curProcess] == 0 {
				curProcess += 1

				if curProcess >= len(processes) {
					curProcess = 0
				}
			}

		}

		//take one away from the current process' time
		//add one to the current burst streak
		procTimes[curProcess] -= 1
		curBurst += 1

		//if a process finishes or a process is premepted
		if procTimes[curProcess] == 0 || (i > 0 && prevProc != curProcess && procTimes[prevProc] != 0) {

			//if the process was preempted, add the preempted process to the gantt
			if i > 0 && prevProc != curProcess && procTimes[prevProc] != 0 {
				waitingTime = serviceTime - (processes[prevProc].ArrivalTime + (processes[prevProc].BurstDuration - procTimes[prevProc]))

				start := serviceTime - TIMESLICE

				gantt = append(gantt, TimeSlice{
					PID:   processes[prevProc].ProcessID,
					Start: start,
					Stop:  serviceTime + 1,
				})
			}

			//if the process finished, add it to the gantt chart, and add it to the schedule
			if procTimes[curProcess] == 0 {

				waitingTime = serviceTime - (processes[curProcess].ArrivalTime + int64(curBurst)) + 1

				start := waitingTime + processes[curProcess].ArrivalTime

				//waitingTime = serviceTime - (processes[curProcess].ArrivalTime + processes[curProcess].BurstDuration) + 1
				totalWait += float64(waitingTime)

				turnaround := serviceTime + 1 - processes[curProcess].ArrivalTime
				totalTurnaround += float64(turnaround)

				completion := serviceTime + 1
				lastCompletion = float64(completion)

				schedule[curProcess] = []string{
					fmt.Sprint(processes[curProcess].ProcessID),
					fmt.Sprint(processes[curProcess].Priority),
					fmt.Sprint(processes[curProcess].BurstDuration),
					fmt.Sprint(processes[curProcess].ArrivalTime),
					fmt.Sprint(waitingTime),
					fmt.Sprint(turnaround),
					fmt.Sprint(completion),
				}

				gantt = append(gantt, TimeSlice{
					PID:   processes[curProcess].ProcessID,
					Start: start,
					Stop:  serviceTime + 1,
				})

			}

		}

	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
