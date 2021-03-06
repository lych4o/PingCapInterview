package mapreduce

import (
    "os"
    "strconv"
    "bufio"
)

var (
    //Channel buffer size of KvBuffer
    KvBufChanBufferSize int = 1024

    //Channel buffer size of Reduce Receiver
    ReduceRecvChanBufferSize int = 1024
)

//Run a mapreduce
func Run(
	inputFileName string,
    outputFileName string,
	nReduce int,
	mapF func(file string, contents string) []KeyValue,
	reduceF func(key string, values []string) string,
) {
    err := os.RemoveAll(MapDir)
    if err != nil { panic(err.Error()) }
    err = os.RemoveAll(ReduceDir)
    if err != nil { panic(err.Error()) }

    inCh, mapShuffleDoneCh := make(chan KeyValue, KvBufChanBufferSize), make(chan bool)
    mapDoneCh := make(chan bool)
    go KvBuffer(inCh, mapShuffleDoneCh, nReduce)
    doMap(inputFileName, nReduce, inCh, mapDoneCh, mapF)
    <-mapDoneCh
    close(inCh)
    <-mapShuffleDoneCh

    reduceResultCh, returnCh := make(chan string, ReduceRecvChanBufferSize), make(chan string)
    go resultRecv(reduceResultCh, returnCh)
    reduceDoneCh := make(chan bool)
    doReduce(nReduce, reduceResultCh, reduceDoneCh, reduceF)
    <-reduceDoneCh
    close(reduceResultCh)
    //result, _ := strconv.ParseInt(<-returnCh, 10, 64)
    result := <-returnCh

    outFile, _ := os.OpenFile(outputFileName, os.O_WRONLY | os.O_TRUNC | os.O_CREATE, 0666)
    defer outFile.Close()
    outFile.WriteString(result)

}

//Run use ExampleMapF and ExampleReduceF
func RunExample(inputFileName string, nReduce int) string{
    outputFileName := "./mr_output"

    Run(inputFileName, outputFileName, nReduce, ExampleMapF, ExampleReduceF)

    inFile, openInErr := os.Open(inputFileName)
    outFile, openOutErr := os.Open(outputFileName)
    defer inFile.Close()
    defer outFile.Close()
    if openInErr != nil { panic(openOutErr.Error()) }
    if openOutErr != nil { panic(openOutErr.Error()) }

    inRd := bufio.NewReader(inFile)
    outRd := bufio.NewReader(outFile)

    outLine, _, rdOutErr := outRd.ReadLine()
    if rdOutErr != nil { panic(rdOutErr.Error()) }
    index, parseErr := strconv.ParseInt(string(outLine), 10, 64)
    if parseErr != nil { panic(parseErr.Error()) }

    ret := ""
    for i:=int64(1); i != index+1; i++ {
        line, _, inRdErr := inRd.ReadLine()
        if inRdErr != nil { panic(inRdErr.Error()) }
        if i == index {
            ret = string(line)
        }
    }
    return ret
}

//Run use ExampleMapF1 and ExampleReduceF
func RunExample1(inputFileName string, nReduce int) string{
    outputFileName := "./mr_output"

    Run(inputFileName, outputFileName, nReduce, ExampleMapF1, ExampleReduceF)

    inFile, openInErr := os.Open(inputFileName)
    outFile, openOutErr := os.Open(outputFileName)
    defer inFile.Close()
    defer outFile.Close()
    if openInErr != nil { panic(openOutErr.Error()) }
    if openOutErr != nil { panic(openOutErr.Error()) }

    inRd := bufio.NewReader(inFile)
    outRd := bufio.NewReader(outFile)

    outLine, _, rdOutErr := outRd.ReadLine()
    if rdOutErr != nil { panic(rdOutErr.Error()) }
    index, parseErr := strconv.ParseInt(string(outLine), 10, 64)
    if parseErr != nil { panic(parseErr.Error()) }

    ret := ""
    for i:=int64(1); i != index+1; i++ {
        line, _, inRdErr := inRd.ReadLine()
        if inRdErr != nil { panic(inRdErr.Error()) }
        if i == index {
            ret = string(line)
        }
    }
    return ret
}
