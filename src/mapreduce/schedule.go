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
	for i := 0; i < ntasks; i++ {
		doTaskArgs := DoTaskArgs{
			JobName:       mr.jobName,
			File:          mr.files[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: nios,
		}
		worker := <-mr.registerChannel
		go dispatchTask(worker, &doTaskArgs, &wg, mr.registerChannel)
		wg.Add(1)
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func dispatchTask(workerName string, doTaskArgs *DoTaskArgs, wg *sync.WaitGroup, registerChannel chan string) bool {
	defer func() {
		go func() {
			registerChannel <- workerName
		}()
		wg.Done()
	}()
	return call(workerName, "Worker.DoTask", *doTaskArgs, nil)
}
