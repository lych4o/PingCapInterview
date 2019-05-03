package mapreduce

import (
    "bufio"
    "os"
    "sort"
    "strconv"
)

//Size of KvBuffer (Bytes).
var KvBufferSize int64 = 1024*1024*64

//SpillRatio.
var SpillRatio float64 = 0.8

//Use to define filename_id.
var spill_round int = 0

type PKV struct {
    Part int
    Key, Value string
}
func (p PKV) toStr() string { return p.Key + " " + p.Value }
func NewPKV(kv KeyValue, mod int) PKV {
    return PKV{
        Part: MHash(kv.Key, mod),
        Key: kv.Key,
        Value: kv.Value,
    }
}

type PKVs []PKV
func (a PKVs) Len() int { return len(a) }
func (a PKVs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a PKVs) Less(i, j int) bool {
    if a[i].Part < a[j].Part { return true
    } else { return a[i].Key <= a[j].Key }
}

func spill(kvs []KeyValue, L int64, R int64, nReduce int, outCh chan string) {
    ret, file, wr := make([]string, nReduce), make([]*os.File, nReduce), make([]*bufio.Writer, nReduce)
    for i:=0; i<nReduce; i++ {
        var openErr error
        ret[i] = strconv.Itoa(i) + "_" + strconv.Itoa(spill_round)
        file[i], openErr = os.OpenFile(ret[i], os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0666)
        if openErr != nil { panic(openErr.Error) }
        wr[i] = bufio.NewWriter(file[i])
        defer file[i].Close()
    }

    arr := make(PKVs, (int64(R-L)+KvBufferSize)%KvBufferSize)
    for i:=0; i<arr.Len(); i++ { arr[i], L = NewPKV(kvs[L], nReduce), int64(L+1)%KvBufferSize }
    sort.Sort(arr)

    for i:=0; i<arr.Len(); i++ {
        _, err := wr[arr[i].Part].WriteString(arr[i].toStr())
        if err != nil { panic(err.Error()) }
    }

    for _, w := range wr { w.Flush() }
    for _, r := range ret { outCh <- r }
}

func KvBuffer(
    inCh chan KeyValue, //Get Key-Value pair.
    outCh chan string, //Output file name.
    nReduce int,
) {
    L, R, size, threshold := int64(0), int64(0), int64(0), int64(float64(KvBufferSize)*SpillRatio)
    buf := make([]KeyValue, KvBufferSize)

    for kv := range inCh {
        buf[R], size, R = kv, size+1, (R+1)%KvBufferSize
        if size >= threshold {
            spill(buf, L, R, nReduce, outCh)
            L, size = R, 0
        }
    }

    if L != R { spill(buf, L, R, nReduce, outCh) }
    close(outCh)
}
