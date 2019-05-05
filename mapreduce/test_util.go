package mapreduce

import (
    "os"
    "math/rand"
    "time"
    //"heap"
    //"container/heap"
)

func init() { rand.Seed(time.Now().UnixNano()) }

//Random input file path.
var InputFileName string = "input.txt"

var (
    //Length of random Key in RandKV.
    RandKeyLen int = 3

    //Lenght of random Value in RandKV.
    RandValLen int = 5
)

//Generate random []Key-Value of length n.
func randKV(n int) []KeyValue {
    ret := make([]KeyValue, n)
    for i:=0; i<n; i++ {
        ret[i] = KeyValue{randStr(RandKeyLen, 4), randStr(RandValLen, 4)}
        //fmt.Printf("kv[%v]: %v\n", i, ret[i].toStr())
    }
    return ret
}

//Generate random string of length n with first k english characters.
func randStr(n, k int) string {
    x := make([]byte, n)
    byteA := []byte("a")[0]
    for i:=0; i<n; i++ { x[i] = byteA + byte(rand.Intn(k)) }
    return string(x)
}

//Generate random file with n words whose length is wordLen, containing first characterNum english characters.
func fileGen(n int, wordLen int, characterNum int) {
    f,_ := os.OpenFile(InputFileName, os.O_WRONLY | os.O_TRUNC | os.O_CREATE, 0666)
    defer f.Close()
    for i:=0; i<n; i++ {
        line := randStr(wordLen, characterNum) + "\n"
        f.WriteString(line)
        //fmt.Printf(line)
    }
}

