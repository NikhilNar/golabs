package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	myWorkers := make(map[string]*WorkerInfo)
	mr.Workers = myWorkers
	totalMapJobs := mr.nMap
	totalReduceJobs := mr.nReduce
	done := false
	for !done {
		select {
		case worker := <-mr.registerChannel:
			mr.Workers[worker] = &WorkerInfo{worker}
		default:
			for worker, _ := range mr.Workers {
				for ; totalMapJobs > 0; totalMapJobs-- {
					args := &DoJobArgs{File: mr.file, Operation: Map, JobNumber: mr.nMap - totalMapJobs, NumOtherPhase: mr.nReduce}
					var reply DoJobReply
					ok := call(worker, "Worker.DoJob", args, &reply)
					if ok == false {
						//wait for a new worker to start or find idle workers
						break
					}

				}

				for ; totalReduceJobs > 0; totalReduceJobs-- {
					args := &DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: mr.nReduce - totalReduceJobs, NumOtherPhase: mr.nMap}
					var reply DoJobReply
					ok := call(worker, "Worker.DoJob", args, &reply)
					if ok == false {
						//wait for a new worker to start or find idle workers
						break
					}

				}

				//exit the map reduce process if all the jobs are finished successfully
				if totalMapJobs == 0 && totalReduceJobs == 0 {
					done = true
					break
				}
			}
			if done {
				break
			}

		}

	}

	return mr.KillWorkers()
}
