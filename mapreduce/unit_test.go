package mapreduce

/*import (
    "testing"
    "os"
    "math/rand"
    "fmt"
    "time"
    //"container/heap"
)*/

/*func TestReduce(t *testing.T) {
    n := 10
    x := randKV(n)
    z := make(KVRs, n)
    y := &z
    for i, _ := range x {
        (*y)[i] = KVR{x[i].Key,x[i].Value, 0}
    }
    heap.Init(y)
    for i:=0; i<10; i++ {
        t.Logf("pop():%v\n",heap.Pop(y))
    }
}*/

/*func TestMap(t *testing.T) {
    err := os.RemoveAll(MapDir)
    if err != nil { panic(err.Error()) }
    err = os.RemoveAll(ReduceDir)
    if err != nil { panic(err.Error()) }

    //n := 15
    //fileGen(n)
    nReduce := 4
    inCh, mapShuffleDoneCh := make(chan KeyValue), make(chan bool)
    mapDoneCh := make(chan bool)
    go KvBuffer(inCh, mapShuffleDoneCh, nReduce)
    doMap(InputFileName, nReduce, inCh, mapDoneCh, ExampleMapF)
    <-mapDoneCh
    close(inCh)
    <-mapShuffleDoneCh

    t.Log("map done")

    reduceResultCh, returnCh := make(chan string), make(chan string)
    go resultRecv(reduceResultCh, returnCh)
    reduceDoneCh := make(chan bool)
    doReduce(nReduce, reduceResultCh, reduceDoneCh, ExampleReduceF)
    <-reduceDoneCh
    close(reduceResultCh)
    index, _ := strconv.ParseInt(<-returnCh, 10, 64)

    ff, _ := os.Open(InputFileName)
    reader := bufio.NewReader(ff)
    for i:=int64(1); ;i++ {
        line, _, _ := reader.ReadLine()
        if i == index {
            t.Logf("First unique word is %v\n", string(line))
            break
        }
    }
}*/

/*func TestKvBuffer(t *testing.T) {
    n, nReduce := 10, 4
    inCh, outCh := make(chan KeyValue, 4), make(chan bool)
    go KvBuffer(inCh, outCh, nReduce)
    for _, kv := range randKV(n) {
        inCh <- kv
    }
    close(inCh)
    <-outCh
    t.Logf("Done!\n")
}*/

/*func TestSpill(t *testing.T) {
    t.Log("fuck")
    KVsize, nReduce := int(9), 3
    kvs := randKV(KVsize)
    outCh := make(chan string)
    go Spill(kvs, 0, int64(KVsize), nReduce, outCh)
    for i:=0; i<nReduce; i++ {
        s := <-outCh
        t.Log(s)
    }

    t.Log("finish")
}*/
