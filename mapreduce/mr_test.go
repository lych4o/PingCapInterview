package mapreduce

import (
    "testing"
    "os"
    "math/rand"
    "fmt"
    "time"
    //"container/heap"
)

func init() { rand.Seed(time.Now().UnixNano()) }

var InputFileName string = "input.txt"
var RandKeyLen int = 1
var RandValLen int = 5

func randStr(n, pre int) string {
    x := ""
    for i:=0; i<n; i++ { x += "a" }
    y := []byte(x)
    for i:=0; i<n; i++ { y[i] += byte(rand.Intn(pre)) }
    return string(y)
}
func randKV(n int) []KeyValue {
    ret := make([]KeyValue, n)
    for i:=0; i<n; i++ {
        ret[i] = KeyValue{randStr(RandKeyLen, 4), randStr(RandValLen, 4)}
        //fmt.Printf("kv[%v]: %v\n", i, ret[i].toStr())
    }
    return ret
}
func fileGen(n int, wordLen int, characterNum int) {
    f,_ := os.OpenFile(InputFileName, os.O_WRONLY | os.O_TRUNC | os.O_CREATE, 0666)
    defer f.Close()
    for i:=0; i<n; i++ {
        line := randStr(wordLen, characterNum) + "\n"
        f.WriteString(line)
        //fmt.Printf(line)
    }
}

type testError struct { info string }
func newTestError(info string) *testError { return &testError{info} }
func (err *testError) Error() string { return err.info }

func singleTest(n int, wordLen int, characterNum int, testId int) error {
    fileGen(n, wordLen, characterNum)
    t1 := time.Now()
    ans1 := RunExample(InputFileName, 4)
    mr_time := time.Since(t1)
    fmt.Printf("mr_time: %v\n", mr_time)
    t1 = time.Now()
    _ = RunExample1(InputFileName, 4)
    mr1_time := time.Since(t1)
    fmt.Printf("mr1_time: %v\n", mr1_time)
    t1 = time.Now()
    ans2 := check(InputFileName)
    check_time := time.Since(t1)
    fmt.Printf("check_time: %v\n", check_time)
    //t.Logf("ans_mr:%v, ans_checker:%v\n", ans1, ans2)
    if ans1 != ans2 { return newTestError(string("Wrong Answer! mr_output:"+ans1+", checker_output:"+ans2))
    } else {
        fmt.Printf("Pass Test %v\n\n", testId)
        return nil
    }
}

/**********************
Modify this function to have a different test
**********************/
func TestRunExample(t *testing.T) {

    //Change variable there to control test
    var (
        testCaseNum int = 5
        wordNum int = 1000000
        wordLen int = 10
        characterNum int = 26
    )

    for i:=0; i<testCaseNum; i++ {
        ok := singleTest(wordNum, wordLen, characterNum, i)
        if ok != nil {
            t.Error(ok.Error())
        }
    }
}
/*func TestGeneral(t *testing.T) {
    t.Log(int64(INT64_MAX))
    x := "12345"
    t.Logf("%v %v\n", len(x), string(x[len(x)-1]))
}*/

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
