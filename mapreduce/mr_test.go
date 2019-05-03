package mapreduce

import (
    "testing"
    "os"
    "math/rand"
    //"strconv"
    "fmt"
    "time"
)

var InputFileName string = "input.txt"

func randStr(n, pre int) string {
    x := ""
    for i:=0; i<n; i++ { x += "a" }
    y := []byte(x)
    for i:=0; i<n; i++ { y[i] += byte(rand.Intn(pre)) }
    return string(y)
}
func getKV(n int) []KeyValue {
    ret := make([]KeyValue, n)
    for i:=0; i<n; i++ {
        ret[i] = KeyValue{randStr(3, 26), randStr(5, 26)}
        fmt.Printf("kv[%v]: %v\n", i, ret[i].toStr())
    }
    return ret
}
func fileGen(n int) {
    f,_ := os.OpenFile(InputFileName, os.O_WRONLY | os.O_TRUNC | os.O_CREATE, 0666)
    defer f.Close()
    for i:=0; i<n; i++ {
        f.WriteString(randStr(4, 5) + "\n")
    }
}

func init() {
    rand.Seed(time.Now().UnixNano())
}

func TestMap(t *testing.T) {
    n, nReduce := 10, 4
    inCh, mapShuffleDoneCh := make(chan KeyValue), make(chan bool)
    mapDoneCh := make(chan bool)
    go KvBuffer(inCh, mapShuffleDoneCh, nReduce)
    fileGen(n)
    doMap(InputFileName, nReduce, inCh, mapDoneCh, ExampleMapF)
    <-mapDoneCh
    close(inCh)
    <-mapShuffleDoneCh
}

/*func TestKvBuffer(t *testing.T) {
    n, nReduce := 10, 4
    inCh, outCh := make(chan KeyValue, 4), make(chan bool)
    go KvBuffer(inCh, outCh, nReduce)
    for _, kv := range getKV(n) {
        inCh <- kv
    }
    close(inCh)
    <-outCh
    t.Logf("Done!\n")
}*/

/*func TestSpill(t *testing.T) {
    t.Log("fuck")
    KVsize, nReduce := int(9), 3
    kvs := getKV(KVsize)
    outCh := make(chan string)
    go Spill(kvs, 0, int64(KVsize), nReduce, outCh)
    for i:=0; i<nReduce; i++ {
        s := <-outCh
        t.Log(s)
    }

    t.Log("finish")
}*/
