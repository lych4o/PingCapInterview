package mapreduce

import (
    //"fmt"
    "io"
    "bufio"
    "os"
)

//Control the max number of thread to run map task.
var ThrdPoolSize int = 4

//Max Bytes of contents to run a map task.
var MaxMapBuffer int64 = 20*1024*1024

//Get map thrd to send ThrdPool.
func getMapThrd(
    inFile string, //mapF input file.
    contents string, //mapF input contents.
    kvBufferCh chan KeyValue, //kvBuffer channel
    mapF func(file string, contents string) []KeyValue, //map function.
) func(chan bool) {
    return func(ch chan bool) {
        kvs := mapF(inFile, contents)
        for _, kv := range kvs { kvBufferCh <- kv }
        ch <- true
    }
}

//Do map on single file.
func doMap(
    inFile string,
    nReduct int,
    kvBufferCh chan KeyValue,
    mapF func(file string, contents string) []KeyValue,
) {
    f, openErr := os.Open(inFile)
    if openErr != nil { panic(openErr.Error()) }
    defer f.Close()

    newThrd, doneCh := make(chan func(chan bool)), make(chan bool)
    pool := NewThrdPool(ThrdPoolSize, newThrd, doneCh)
    go pool.Run()

    reader := bufio.NewReader(f)
    var contents string = ""
    for {
        line, _, rdErr := reader.ReadLine()
        if rdErr == io.EOF { break; }
        if int64(len(line) + 1 + len(contents)) > MaxMapBuffer {
            thrd := getMapThrd(inFile, contents, kvBufferCh, mapF)
            newThrd <- thrd
            contents = ""
        }
        contents += string(line)+"\n"
    }
}
