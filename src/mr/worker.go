package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
	"io/ioutil"
	"sort"
	"os"
	"strconv"
	"encoding/json"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(mapf func(string, string) []KeyValue, reply Reply) {
	filePath := reply.FilePath
	nReduce := reply.NReduce
	id := reply.Id

	// 1. Read the file content and invoke mapf
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		file.Close()
		log.Fatalf("cannot read %v", filePath)
	}
	file.Close()
	kva := mapf(filePath, string(content))

	// 2. Divide kv pairs into nReduce buckets
	buckets := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		buckets[i] = []KeyValue{}
	}
	for _, kv := range kva {
		ind := ihash(kv.Key) % nReduce
		buckets[ind] = append(buckets[ind], kv)
	}

	// 3. Create intermediate file mr-X-Y
	for i, bucket := range buckets {
		interPath := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)
		// create a temporary file mr-X-Y.tmp
		tmpFile, err := os.CreateTemp("", interPath + ".tmp")
		if err != nil {
			log.Fatalf("cannot create tmp file");
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Encode failed");
			}
		}
		tmpFile.Close()
		// atomically rename the temporary file
		os.Rename(tmpFile.Name(), interPath)
	}

	// 4. Send done message to the coordinator
	args := Args{id, -1}
	replyMessage := Reply{}
	call("Coordinator.MapTaskDone", &args, &replyMessage)
}

func doReduceTask(reducef func(string, []string) string, reply Reply) {
	nMap := reply.NMap
	id := reply.Id

	// 1. Read the content of file mr-*-Y
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		filePath := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(id)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 2. Sort the intermediate
	sort.Sort(ByKey(intermediate))

	// 3. Invoke the reducef and organize the output into mr-out-Y
	oname := "mr-out-" + strconv.Itoa(id)
	ofile, _ := os.Create(oname)
	// ATTENTION: No i++
	// because i = j jumps through the whole group
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()

	// 4. Send done message to the coordinator
	args := Args{-1, id}
	replyMessage := Reply{}
	call("Coordinator.ReduceTaskDone", &args, &replyMessage)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.AllocateTask", &args, &reply)
		if !ok {
			log.Fatalf("coordinator failed")
			break
		}
		if reply.ReplyType == 1 {
			doMapTask(mapf, reply)
		} else if reply.ReplyType == 2 {
			doReduceTask(reducef, reply)
		} else if reply.ReplyType == 3 {
			// all works done
			break
		} else {
			// no task allocated, sleep for a while
			time.Sleep(time.Second)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
