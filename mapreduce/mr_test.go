package mapreduce

import (
    "testing"
    "strings"
    "os"
    "math/rand"
    "io"
    "bufio"
    "strconv"
    "fmt"
    "sort"
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
func getKV(n int) []KeyValue {
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

type sa []string
func (a sa) Len() int { return len(a) }
func (a sa) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sa) Less(i, j int) bool { return a[i] < a[j] }

func check(fileName string) string {
    arr := make(sa, 10000)[:0]
    f, err := os.Open(fileName)
    if err != nil { panic(err.Error()) }
    defer f.Close()
    rd := bufio.NewReader(f)

    for i:=1; ;i++{
        line, _, rdErr := rd.ReadLine()
        if rdErr == io.EOF { break }
        arr = append(arr, string(line)+" "+strconv.FormatInt(int64(i), 10))
    }

    sort.Sort(arr)

    ans, min := "", int64(INT64_MAX)
    for i,j:=0,0; i<arr.Len(); i=j {
        kv := strings.Split(arr[i], " ")
        k, v := kv[0], kv[1]
        for j=i+1; j<arr.Len(); j++ {
            jkv := strings.Split(arr[j], " ")
            jk, _ := jkv[0], jkv[1]
            if jk != k { break }
        }
        if j>i+1 { continue }
        idx, parseErr := strconv.ParseInt(v, 10, 64)
        if parseErr != nil { panic(parseErr.Error()) }
        if idx < min { ans, min = k, idx }
    }

    return ans
}

type testError struct { info string }
func newTestError(info string) *testError { return &testError{info} }
func (err *testError) Error() string { return err.info }

func singleTest(n int, wordLen int, characterNum int, testId int) error {
    fileGen(n, wordLen, characterNum)
    t1 := time.Now()
    ans1 := RunExample(InputFileName, 4)
    mr_time := time.Since(t1)
    t1 = time.Now()
    ans2 := check(InputFileName)
    check_time := time.Since(t1)
    //t.Logf("ans_mr:%v, ans_checker:%v\n", ans1, ans2)
    if ans1 != ans2 { return newTestError(string("Wrong Answer! mr_output:"+ans1+", checker_output:"+ans2))
    } else {
        fmt.Printf("Pass Test %v, mr_time: %v, check_time: %v\n", testId, mr_time, check_time)
        return nil
    }
}

func TestRunExample(t *testing.T) {
    for i:=0; i<1; i++ {
        ok := singleTest(100000, 6, 26, i)
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
    x := getKV(n)
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
