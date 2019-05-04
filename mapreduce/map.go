package mapreduce

import (
    //"fmt"
    "strings"
    "strconv"
)

func ExampleMapF(lineIdx string, contents string) []KeyValue {
    //fmt.Printf("in map\n")
    lines := strings.Split(contents, "\n")
    startIdx, parseErr := strconv.ParseInt(lineIdx, 10, 64)
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

func ExampleMapF1(lineIdx string, contents string) []KeyValue {
    //fmt.Println("map in")
    startIdx, parseErr := strconv.ParseInt(lineIdx, 10, 64)
    if parseErr != nil { panic(parseErr.Error()) }

    ret := make([]KeyValue, 10000)
    ret = ret[:0]

    var byteLn byte = []byte("\n")[0]
    for L,R,i:=int64(0),int64(0),int64(0); L<int64(len(contents)); L,i=R+1,i+1{
        for R=L+1; R<int64(len(contents)) && contents[R]!=byteLn; R++ {}
        word := contents[L:R]
        if word == "" { continue }
        ret = append(ret, KeyValue{word, strconv.FormatInt(startIdx+i, 10)})
    }
    return ret
}
