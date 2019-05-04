package mapreduce

import (
    "os"
    //"fmt"
    "strconv"
    "bufio"
)

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

    inCh, mapShuffleDoneCh := make(chan KeyValue), make(chan bool)
    mapDoneCh := make(chan bool)
    go KvBuffer(inCh, mapShuffleDoneCh, nReduce)
    doMap(inputFileName, nReduce, inCh, mapDoneCh, mapF)
    <-mapDoneCh
    close(inCh)
    <-mapShuffleDoneCh

	//fmt.Println("Map Done!")

    reduceResultCh, returnCh := make(chan string), make(chan string)
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

    //fmt.Println("Reduce Done!")
}

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
            //fmt.Printf("The first unique word is %v\n", string(line))
            ret = string(line)
        }
    }
    return ret
}
