package mr

import (
	"encoding/json"
	"fmt"
	"github.com/rexlien/go-utils/xln-utils/common"
	priority_queue "github.com/rexlien/go-utils/xln-utils/container"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"

/*
type Comparable interface {

	 Less(j Comparable) bool
}


// An Item is something we manage in a priority queue.
type Item struct {

	index int
	Value Comparable

}

func (item *Item) SetIndex(index int) {
	item.index = index
}

func (item *Item) Index() int {
	return item.index
}


// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].Value.Less(pq[j].Value)//Priority() > pq[j].Priority()
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].SetIndex(i)
	pq[j].SetIndex(j)
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	//item.index = n
	item.SetIndex(n)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.SetIndex(-1) // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item) {
	//item.SetValue(value)
	//item.SetPriority(priority)
	//item.SetKey(key)
	heap.Fix(pq, item.Index())
}

func NewPriorityQueue() *PriorityQueue {
	newPq := make(PriorityQueue, 0)
	heap.Init(&newPq)
	return &newPq
}
*/
// This example creates a PriorityQueue with some items, adds and manipulates an item,
// and then removes the items in priority order.

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string

}

func (kv *KeyValue) Less(dst common.Comparable) bool {

	return kv.Key < dst.(*KeyValue).Key
}

type IndexedKeyValue struct {

	KeyValue *KeyValue
	index int
}

func (ikv *IndexedKeyValue) Less(dst common.Comparable) bool {

	return ikv.KeyValue.Less(dst.(*IndexedKeyValue).KeyValue)
}



type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}





//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//Reduce(reducef, 8, 9)

	// uncomment to send the Example RPC to the master.
	// CallExample()
	//go func() {
	for {
		CallGetMapTask(mapf)
		/*
		for {
			if !more {
				break
			}
			more = CallGetMapTask(mapf)
		}
*/
		CallGetReduceTask(reducef)
		/*
		for {
			if !more {
				break
			}
			more = CallGetReduceTask(reducef)
		}
		 */
		time.Sleep(2* time.Second)
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallGetMapTask(mapf func(string, string) []KeyValue) bool {

	// declare an argument structure.
	args := GetMapperRequest{}

	reply := GetMapperResponse{}

	// send the RPC request, wait for the reply.
	if call("Master.GetMapper", &args, &reply) {

		fmt.Printf("Reply: %s : %d\n", reply.FileName, reply.MapperID)

		filename := reply.FileName
		file, err := os.Open(reply.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		var intermediate []KeyValue
		intermediate = append(intermediate, kva...)

		//hashes files into out
		sort.Sort(ByKey(intermediate))

		numReducer := reply.NumReducer
		mapperID := reply.MapperID

		reduceMap := make(map[int][]KeyValue)

		for _, kv := range intermediate {
			hashIndex := ihash(kv.Key) % numReducer
			reduceMap[hashIndex] = append(reduceMap[hashIndex], kv)

		}

		for i:=0; i < numReducer; i++ {

			outFileName := "output/mr-" + strconv.Itoa(mapperID) + "-" + strconv.Itoa(i) + ".json"
			outFile, err := os.Create(outFileName)
			if err != nil {
				return false
			}

			enc := json.NewEncoder(outFile)
			kva := reduceMap[i]
			for _, kv := range kva {
				enc.Encode(&kv)
			}
			fmt.Printf("file : %s output", outFileName)
			_ = outFile.Close()
		}

		req := DoneMapRequest{ FileName: reply.FileName, MapperID: reply.MapperID, ProgressID: reply.ProgressID}

		reply := DoneMapReply{}
		if !call("Master.DoneMap", &req, &reply) {
			return false
		}


		return true
	} else {
		return false
	}

	// reply.Y should be 100.
	//fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallGetReduceTask(reducef func(string, []string) string) bool {

	args := GetReducerRequest{}
	reply := GetReducerReply{}

	if call("Master.GetReducer", &args, &reply) {

		mapperCount := reply.NumMapper
		reducerID := reply.ID

		success := Reduce(reducef, mapperCount, reducerID)
		if success {
			args := DoneReduceRequest{ID: reply.ID, ProgressID: reply.ProgressID}
			call("Master.DoneReduce", &args, &DoneReduceReply{})
		} else {

		}

	}

	return true
}

func Reduce(reducef func(string, []string) string, mapperCount, reducerID int) bool {



	inDecoder := make([]*json.Decoder, mapperCount)
	for i:= 0; i< mapperCount; i++ {

		fileName := fmt.Sprintf("output/mr-%d-%d.json", i, reducerID)
		inFile, err := os.Open(fileName)
		if err != nil {
			return false
		}

		inDecoder[i] = json.NewDecoder(inFile)
		//kv := KeyValue{}
		//dec.Decode(&kv)
	}

	pq := priority_queue.NewPriorityQueue()
	for i:= 0; i < mapperCount; i++ {


		//for {
		/*
			var kv KeyValue
			err := inDecoder[i].Decode(&kv)

			if err != nil {
				if  err == io.EOF {
					break
				} else {
					fmt.Printf("Decode error: %s", err.Error())
					panic("Decode error")
				}
			}
			heap.Push(pq, &Item{
				Value: &IndexedKeyValue{KeyValue: &kv, index: i},
			})
		//}

		 */
		if !DecodeToPQ(inDecoder[i], i, pq) {
			break
		}
	}

	oname := fmt.Sprintf("output/mr-out-%d", reducerID)
	ofile, _ := os.Create(oname)

	//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	values := make([]string, 0, 8)
	var prevKey *string = nil
	for {

		if pq.Len() == 0 {
			if len(values) != 0 && prevKey != nil {
				result := reducef(*prevKey, values)
				_, err := fmt.Fprintf(ofile, "%v %v\n", *prevKey, result)
				if err != nil {
					fmt.Printf("output write error: %s", err.Error())
				}
			}
			break
		}

		front := pq.Dequeue()//heap.Pop(pq)

		//if()

		ikv := front.(*IndexedKeyValue)
		kv := ikv.KeyValue

			if  prevKey != nil && *prevKey != kv.Key {
				result := reducef(*prevKey, values)
				_, err := fmt.Fprintf(ofile, "%v %v\n", *prevKey, result)
				if err != nil {
					fmt.Printf("output write error: %s", err.Error())
				}
				values = values[:0]
			}

		nextDecoder := inDecoder[ikv.index]
		DecodeToPQ(nextDecoder, ikv.index, pq)

		values = append(values, kv.Value)
		prevKey = &kv.Key

		//fmt.Printf("Key: %s, Value %s\n", kv.Key, kv.Value)

		//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	}

	return true
}

func DecodeToPQ(decoder *json.Decoder, index int, pq *priority_queue.PriorityQueue) bool {

	var kv KeyValue
	err := decoder.Decode(&kv)

	if err != nil {
		if  err == io.EOF {
			return false
		} else {
			fmt.Printf("Decode error: %s", err.Error())
			panic("Decode error")
		}
	}
	//heap.Push(pq, &Item{
	//	Value: &IndexedKeyValue{KeyValue: &kv, index: index},
	//})
	pq.Enqueue(&IndexedKeyValue{KeyValue: &kv, index: index})
	return true
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Print("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
