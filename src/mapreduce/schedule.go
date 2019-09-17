package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	// read workers from channel

	wg := sync.WaitGroup{}
	taskChan := make(chan int)
	go mr.TaskRunner(ntasks, taskChan, &wg, phase, nios)
	for i := 0; i < ntasks; i++ {
		taskChan <- i
		wg.Add(1)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master) TaskRunner(nTasks int, taskChan chan int, wg *sync.WaitGroup, phase jobPhase, nios int) {
	remainTasksCnt := nTasks
	for remainTasksCnt > 0 {
		select {
		case taskId := <-taskChan:
			doTaskArgs := DoTaskArgs{
				JobName:       mr.jobName,
				File:          mr.files[taskId],
				Phase:         phase,
				TaskNumber:    taskId,
				NumOtherPhase: nios,
			}
			worker := <-mr.registerChannel
			result := dispatchTask(worker, &doTaskArgs, wg, mr.registerChannel, taskChan, taskId)
			if result {
				remainTasksCnt--
			}
		default:
			if remainTasksCnt <= 0 {
				break
			}
		}
	}
}

func dispatchTask(workerName string, doTaskArgs *DoTaskArgs, wg *sync.WaitGroup, registerChannel chan string, taskChan chan int, taskId int) bool {
	result := call(workerName, "Worker.DoTask", *doTaskArgs, nil)
	if result {
		go func() {
			registerChannel <- workerName
		}()
		wg.Done()
	} else {
		fmt.Printf("Task %d failed. Retry.\n", taskId)
		go func() {
			taskChan <- taskId
		}()
	}
	return result
}
