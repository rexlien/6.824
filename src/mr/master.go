package mr

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState int

const Done TaskState = 0
const Queueing TaskState = 1
const Waiting TaskState = 2

type Mapper struct {

	ID int
	FileName string
	NextProgressID int32

	doneChannels map[int32]chan bool
	State atomic.Value

}

func newMapper(id int, fileName string) *Mapper {

	state := atomic.Value{}
	state.Store(Queueing)

	return &Mapper{ID: id,  FileName: fileName, State: state, doneChannels: make(map[int32]chan bool)}

}


type reducer struct {

	id int
	NextProgressID int32

	State atomic.Value
	doneChannels map[int32]chan bool
}

func (r *reducer) Id() int {
	return r.id
}

func newReducer(id int) *reducer {

	state := atomic.Value{}
	state.Store(Queueing)

	return &reducer{id: id, State: state, doneChannels: make(map[int32]chan bool)}

}


type RequestTask struct {

	request interface{}
	reply interface{}
}


type Job struct  {

	NumReducer int
	NumMapper int

	Mappers sync.Map

	mapperChannel     chan *Mapper
	doneMapperCount   sync.WaitGroup
	doneMapperChannel chan *Mapper

	Reducers sync.Map

	reducerChannel chan *reducer
	doneReducerCount sync.WaitGroup

	//doneChannel chan bool

}


func NewJob(numReducer int, numMapper int) *Job {

	newJob := &Job { Mappers: sync.Map{}, Reducers: sync.Map{}, mapperChannel: make(chan *Mapper, numMapper), NumReducer: numReducer, NumMapper: numMapper, doneMapperCount: sync.WaitGroup{}}
	newJob.doneMapperChannel = make(chan *Mapper)
	newJob.doneMapperCount.Add(numMapper)
	newJob.reducerChannel = make(chan *reducer, numReducer)
	newJob.doneReducerCount.Add(numReducer)

	//start mapper monitor progress
	go func() {

		for {
			select {
				case mapper := <- newJob.doneMapperChannel:
					fmt.Printf("Mapper Done : %d\n", mapper.ID)
					mapper.State.Store(Done)
					newJob.doneMapperCount.Done()

				default:
					time.Sleep(time.Second)

			}
		}

	}()

	go func() {

		newJob.doneMapperCount.Wait()

		fmt.Printf("All Mapper Done, Start reducer\n")

		for i:=0; i < newJob.NumReducer; i++ {
			newReducer := newReducer(i)
			newJob.Reducers.Store(i, newReducer)
			newJob.reducerChannel <- newReducer
		}



	}()


	return newJob

}


type Master struct {
	// Your definitions here.
	job *Job

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) GetMapper(request *GetMapperRequest, reply *GetMapperResponse) error {

	select {
		case mapper := <- m.job.mapperChannel:
			reply.NumReducer = m.job.NumReducer
			reply.FileName = mapper.FileName
			reply.MapperID = mapper.ID

			curProgressID := mapper.NextProgressID
			reply.ProgressID = curProgressID
			mapper.NextProgressID++
			//atomic.AddInt32(&mapper.ProgressID, 1)

			mapper.doneChannels[curProgressID] = make(chan bool)
			//mapper.doneChannel.
			mapper.State.Store(Waiting)


			//wait at max 30 second
			go func() {
				//for {
					select {

						case done := <- mapper.doneChannels[curProgressID]:
							if done {
								mapper.State.Store(Done)
								m.job.doneMapperChannel <- mapper
							}
						case <- time.After(30 * time.Second):
							//timeout reset, close
							mapper.doneChannels[curProgressID] = nil
							mapper.State.Store(Queueing)
							m.job.mapperChannel <- mapper

					}

				//}
			}()
			return nil
		default:
			return errors.New("no mapper task left")
	}
}

func (m *Master) DoneMap(request *DoneMapRequest, reply *DoneMapReply) error {

	val, ok := m.job.Mappers.Load(request.MapperID)
	if !ok || val == nil   {
		panic("Mapper not exist")
	}
	mapper := val.(*Mapper)
	doneChannel := mapper.doneChannels[request.ProgressID]
	if doneChannel != nil {
		select {
			case doneChannel <- true:
				fmt.Printf("Mapper Progress Done %d\n", mapper.ID)
			case <- time.After(3 * time.Second):
				return errors.New("progress already timed out")
		}
	}

	return nil
}


func (m *Master) GetReducer(request *GetReducerRequest, reply *GetReducerReply) error {


	select {
		case reducer := <- m.job.reducerChannel:
			reply.ID = reducer.id
			reply.NumMapper = m.job.NumMapper
			reply.ProgressID = reducer.NextProgressID
			curProgressID := reducer.NextProgressID
			reducer.NextProgressID++
			reducer.State.Store(Waiting)
			reducer.doneChannels[curProgressID] = make(chan bool)

			go func() {

				select {

					case done := <- reducer.doneChannels[curProgressID]:
						if done {
							reducer.State.Store(Done)
							m.job.doneReducerCount.Done()
						}
					case <- time.After(30 * time.Second):
						reducer.doneChannels[curProgressID] = nil
						reducer.State.Store(Queueing)
						m.job.reducerChannel <- reducer
				}

			}()

		case <- time.After(3 * time.Second):
			return errors.New("no Reducer task left")
	}

	return nil

}

func (m *Master) DoneReduce(request *DoneReduceRequest, reply *DoneReduceReply) error {

	val, ok := m.job.Reducers.Load(request.ID)
	if !ok || val == nil   {
		panic("Mapper not exist")
	}
	reducer := val.(*reducer)
	doneChannel := reducer.doneChannels[request.ProgressID]
	if doneChannel != nil {
		select {
		case doneChannel <- true:
			fmt.Printf("Reducer Progress Done %d\n", reducer.id)
		case <- time.After(3 * time.Second):
			return errors.New("progress already timed out")
		}
	}
	return nil
}




//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.job.doneReducerCount.Wait()
	ret = true

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	fileLength := 0
	for _, file := range files {

		matchingFiles, error := filepath.Glob(file)
		if error != nil {
			panic("matching glob fail")
		}


		fileLength += len(matchingFiles)
	}


	m := Master{job: NewJob(nReduce, fileLength)}

	fileID := 0
	// Your code here.
	for _, file := range files {

		matchingFiles, error := filepath.Glob(file)
		if error != nil {
			panic("matching glob fail")
		}

		for _, filename := range matchingFiles {

			newMapper := newMapper(fileID, filename)
			m.job.Mappers.Store(fileID, newMapper)
			m.job.mapperChannel <- newMapper
			fileID++
		}
	}


	m.server()
	return &m
}
