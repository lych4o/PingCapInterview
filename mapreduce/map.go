package mapreduce

import (
    //"fmt"
    "strings"
    "strconv"
)

func ExampleMapF(lineIdx string, contents string) []KeyValue {
    lines := strings.Split(contents, "\n")
    startIdx, parseErr := strconv.ParseInt(lineIdx, 10, 64)
    //fmt.Printf("Enter MapF %v\n", startIdx)
    if parseErr != nil { panic(parseErr.Error()) }

    ret := make([]KeyValue, 10000)
    ret = ret[:0]

    for i:=int64(0); i<int64(len(lines)); i++ {
        word := lines[i]
        if word == "" { continue }
        ret = append(ret, KeyValue{word, strconv.FormatInt(startIdx+i, 10)})
    }

    return ret
}
