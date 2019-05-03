package mapreduce

import (
    //"fmt"
    "io"
    "bufio"
    "os"
    "strconv"
)

var (
    //Control the max number of thread to run map task.
    ThrdPoolSize int = 4

    //Max Bytes of contents to run a map task.
    MaxMapBuffer int64 = 20*1024*1024

    //Buffer size of thread pool channel
    ThrdBuffer int = 1024
)

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
    doneCh chan bool,
    mapF func(
        //file string,
        lineIdx string, //Begin line index
        contents string,
    ) []KeyValue,
) {
    f, openErr := os.Open(inFile)
    if openErr != nil { panic(openErr.Error()) }
    defer f.Close()

    newThrd := make(chan func(chan bool), ThrdBuffer)
    pool := NewThrdPool(ThrdPoolSize, newThrd, doneCh)
    go pool.Run()

    reader := bufio.NewReader(f)
    var contents string = ""
    for i, pre:=int64(1), int64(1); ;i++{
        line, _, rdErr := reader.ReadLine()
        if rdErr == io.EOF || int64(len(line) + 1 + len(contents)) > MaxMapBuffer {
            thrd := getMapThrd(strconv.FormatInt(pre, 10), contents, kvBufferCh, mapF)
            newThrd <- thrd
            contents, pre = "", i+1
            if rdErr == io.EOF { break }
        }
        contents += string(line)+"\n"
    }
    close(newThrd)
}
