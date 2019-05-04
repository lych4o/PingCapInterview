package mapreduce

import (
    "strings"
    "fmt"
    "io"
    "container/heap"
    "strconv"
    "bufio"
    "os"
    "io/ioutil"
)

var (

)

type KVR struct {
    Key, Value string
    FileIdx int
}
func (kvr KVR) toStr() string { return kvr.Key + " " + kvr.Value }
func Line2KVR(line string, FileIdx int) KVR {
    kv := strings.Split(line, " ")
    return KVR{
        Key: kv[0],
        Value: kv[1],
        FileIdx: FileIdx,
    }
}
type KVRs []KVR
func (a KVRs) Len() int { return len(a) }
func (a KVRs) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a KVRs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a *KVRs) Push(kvr interface{}) { *a = append(*a, kvr.(KVR)) }
func (a *KVRs) Pop() interface{} {
    old := *a
    n := len(old)
    ret := old[n-1]
    *a = old[0: n-1]
    return ret
}

func getPartition(file os.FileInfo) int {
    x, parseErr := strconv.ParseInt(strings.Split(file.Name(), "_")[0], 10, 32)
    if parseErr != nil {
        fmt.Printf("map output filename format wrong!\n")
        panic(parseErr.Error())
    }
    return int(x)
}

func getMapOutFile(nReduce int) [][]string {
    ret := make([][]string, nReduce)
    for i:=0; i<nReduce; i++ { ret[i] = make([]string, 100) }

    files, dirErr := ioutil.ReadDir(MapDir)
    if dirErr != nil { panic(dirErr.Error()) }

    for _, file := range files {
        if file.IsDir() { continue }
        part := getPartition(file)
        ret[part] = append(ret[part], MapDir + "/" + file.Name())
    }

    return ret
}

func MergeFiles(src []string, dst string) {
    srcF := make([]*os.File, len(src))
    srcRd := make([]*bufio.Reader, len(src))
    var openError error
    for i, fname := range src {
        srcF[i], openError = os.Open(fname)
        if openError != nil { panic(openError.Error()) }
        srcRd[i] = bufio.NewReader(srcF[i])
    }

    kvrs := make(KVRs, len(src))
    kvrPQ := &kvrs
    for i, rd := range srcRd {
        line, _, rdErr := rd.ReadLine()
        if rdErr == io.EOF {
            *kvrPQ = (*kvrPQ)[:len(*kvrPQ)-1]
            continue
        }
        *kvrPQ = append(*kvrPQ, Line2KVR(string(line), i))
    }
    heap.Init(kvrPQ)

    nowKey := ""
    dstF, openErr := os.OpenFile(dst, os.O_WRONLY | os.O_TRUNC | os.O_CREATE, 0666)
    if openErr != nil { panic(openErr.Error()) }
    dstWr := bufio.NewWriter(dstF)
    for len(*kvrPQ) > 0 {
        top := heap.Pop(kvrPQ).(KVR)
        if top.Key != nowKey {
            nowKey = top.Key
            dstWr.WriteString(nowKey + ":\n")
        }
        dstWr.WriteString(top.Value)
        line, _, rdErr := srcRd[top.FileIdx].ReadLine()
        if rdErr != io.EOF { heap.Push(kvrPQ, Line2KVR(string(line), top.FileIdx)) }
    }
    dstWr.Flush()
}

func getReduceThrd(
    key string,
    values []string,
    resultCh chan string,
    reduceF func(key string, values []string) string,
) func(chan bool) {
    return func(ch chan bool) {
        result := reduceF(key, values)
        resultCh <- result
        ch <- true
    }
}

func doReduce(
    nReduce int,
    resultCh chan string,
    reduceF func(key string, values []string) string,
) {
    files := getMapOutFile(nReduce)
    reduceIn := make([]string, nReduce)
    for i, fs := range files {
        dstName := ReduceDir + "/" + strconv.FormatInt(int64(i), 10) + ".reduceIn"
        MergeFiles(fs, dstName)
        reduceIn[i] = dstName
    }

    newThrd := make(chan func(chan bool), ThrdBuffer)
    doneCh := make(chan bool)
    pool := NewThrdPool(ThrdPoolSize, newThrd, doneCh)
    go pool.Run()

    for i:=0; i<nReduce; i++ {
        vals := make([]string, 1024*1024)[:0]
        key := ""
        redInFile, openErr := os.Open(reduceIn[i])
        if openErr != nil { panic(openErr.Error()) }
        rd := bufio.NewReader(redInFile)
        for {
            line, _ , rdErr := rd.ReadLine()
            if rdErr == io.EOF || string(line[len(line)-1:]) == ":" {
                if key != "" { newThrd <- getReduceThrd(key, vals, resultCh, reduceF) }
                vals, key = vals[:0], string(line[:len(line)-1])
                if rdErr == io.EOF { break }
            } else { vals = append(vals, string(line)) }
        }
    }
    close(newThrd)
}
