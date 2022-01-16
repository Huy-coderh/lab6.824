package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	args := RpcArgs{}
	count := 0
	for {
		reply := RpcReply{}
		call("Coordinator.RpcHandler", &args, &reply)
		//fmt.Println(reply)
		if len(reply.TaskName) == 0 {
			if count >= 11 {
				break
			}
			count++
			args.TaskName = ""
			time.Sleep(time.Duration(1) * time.Second)
		} else {
			phase := reply.Phase
			if phase == 1 {
				doMap(mapf, reply.TaskName, reply.Index, reply.NReduce)
			} else {
				doReduce(reducef, reply.TaskName)
			}
			args.TaskName = reply.TaskName
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func doMap(mapf func(string, string) []KeyValue, taskName string, index int, nReduce int) {
	file, err := os.Open(taskName)
	if err != nil {
		log.Fatalf("cannot open %v", taskName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", taskName)
	}
	file.Close()

	// 初始化nReduce个buckets
	buckets := [][]KeyValue{}
	for i := 0; i < nReduce; i++ {
		buckets = append(buckets, []KeyValue{})
	}
	// 分桶
	for _, kv := range mapf(taskName, string(content)) {
		keyIndex := ihash(kv.Key) % nReduce
		buckets[keyIndex] = append(buckets[keyIndex], kv)
	}
	// 存储至文件
	for i, kvs := range buckets {
		if len(kvs) == 0 {
			continue
		}
		fileName := "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(i+1)
		if fInfo, err := os.Stat(fileName); fInfo != nil && err == nil {
			continue
		}
		f, err := ioutil.TempFile("./", "")
		if err != nil {
			continue
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		for _, kv := range kvs {
			enc.Encode(&kv)
		}
		os.Rename(f.Name(), fileName)
	}
}

func doReduce(reducef func(string, []string) string, taskName string) {
	if !strings.HasPrefix(taskName, "reduce_") {
		return
	}
	reduceIndex, err := strconv.Atoi(strings.TrimPrefix(taskName, "reduce_"))
	if err != nil {
		return
	}

	fs, err := ioutil.ReadDir("./")
	if err != nil {
		return
	}

	intermediate := []KeyValue{}
	// 处理当前目录下所有reduceIndex文件
	pattern := "^mr-(\\d)+-" + strconv.Itoa(reduceIndex) + "$"
	for _, file := range fs {
		if res, _ := regexp.MatchString(pattern, file.Name()); res && !file.IsDir() {
			f, err := os.Open(file.Name())
			if err != nil {
				fmt.Println("can't open " + file.Name())
				continue
			}
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			f.Close()
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reduceIndex)
	if fInfo, err := os.Stat(oname); fInfo != nil && err == nil {
		return
	}
	ofile, _ := ioutil.TempFile("./", "")
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)

}
