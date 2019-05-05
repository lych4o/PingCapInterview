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

//Generate random string of length n with first k english characters.
func RandStr(n, k int) string {
    x := make([]byte, n)
    byteA := []byte("a")[0]
    for i:=0; i<n; i++ { x[i] = byteA + byte(rand.Intn(k)) }
    return string(x)
}

//Generate random []Key-Value of length n with Key of length keyLen, Value of length valLen.
func RandKV(n, keyLen, valLen int) []KeyValue {
    ret := make([]KeyValue, n)
    for i:=0; i<n; i++ {
        ret[i] = KeyValue{RandStr(keyLen, 4), RandStr(valLen, 4)}
        //fmt.Printf("kv[%v]: %v\n", i, ret[i].toStr())
    }
    return ret
}

//Generate random file with n words whose length is wordLen, containing first characterNum english characters.
func FileGen(n int, wordLen int, characterNum int) {
    f,_ := os.OpenFile(InputFileName, os.O_WRONLY | os.O_TRUNC | os.O_CREATE, 0666)
    defer f.Close()
    for i:=0; i<n; i++ {
        line := RandStr(wordLen, characterNum) + "\n"
        f.WriteString(line)
        //fmt.Printf(line)
    }
}

