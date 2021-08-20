package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	no        int
	started   time.Time
	processed time.Time
}

type Worker struct {
	no    int
	count int
}

var wg sync.WaitGroup
var jobChan chan Job
var r *rand.Rand
var workers []*Worker

func main() {

	jobChan = make(chan Job, 10)
	rs := rand.NewSource(time.Now().UnixNano())
	r = rand.New(rs)

	// start workload simulator
	go workloadSimulator()

	// creae 3 workers
	for i := 1; i < 4; i++ {
		wg.Add(1)
		w := Worker{no: i}
		workers = append(workers, &w)
		go worker(&w)
	}

	// prevent the app exiting before jobs are processed or timeout
	if waitTimeout(&wg, time.Second*5) {
		fmt.Println("Time is up for workers!")
	}

	dumpWorkers()
}

func worker(w *Worker) {
	defer wg.Done()

	for job := range jobChan {
		process(&job, w)
	}
}

func process(job *Job, w *Worker) {
	job.processed = time.Now().UTC()
	w.count++

	fmt.Printf("Job #%v is processed by worker #%v remaining %v \n", job.no, w.no, len(jobChan))

	// Make each worker's job processing times a little different.
	// As worker numbers increase; job processing slows down.
	time.Sleep(time.Duration(r.Intn(300)+(w.no*150)) * time.Millisecond)
}

func workloadSimulator() {
	for i := 1; i < 100; i++ {
		time.Sleep(time.Millisecond * time.Duration(r.Intn(100)+20))

		jobChan <- Job{no: i, started: time.Now().UTC()}
	}

	// to stop the worker, first close the job channel
	close(jobChan)
}

func dumpWorkers() {
	for _, w := range workers {
		fmt.Printf("Worker #%v processed %v items\n", w.no, w.count)
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	select {
	case <-ch:
		return false
	case <-time.After(timeout):
		return true
	}
}
