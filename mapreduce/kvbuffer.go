package mapreduce

import (
    "bufio"
    "os"
    "sort"
    "strconv"
)

func init() {
    spill_round = 0
}

var (
    //Size of KvBuffer (Bytes).
    KvBufferSize int64 = 4*1024*1024*1024

    //SpillRatio.
    SpillRatio float64 = 0.8

    //Use to define filename_id.
    spill_round int = 0
)

//Use to sort.
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
    if a[i].Part != a[j].Part { return a[i].Part < a[j].Part
    } else { return a[i].Key < a[j].Key }
}

//Write buffer to files.
func spill(kvs []KeyValue, L int64, R int64, nReduce int) {
    ret := make([]string, nReduce)
    file := make([]*os.File, nReduce)
    wr := make([]*bufio.Writer, nReduce)

    for i:=0; i<nReduce; i++ {
        var openErr error
        ret[i] = MapDir + strconv.Itoa(i) + "_" + strconv.Itoa(spill_round) + ".mapOut"
        file[i], openErr = os.OpenFile(ret[i], os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0666)
        if openErr != nil { panic(openErr.Error()) }
        defer file[i].Close()
        wr[i] = bufio.NewWriter(file[i])
    }

    arr := make(PKVs, (int64(R-L)+KvBufferSize)%KvBufferSize)
    for i:=0; i<arr.Len(); i++ { arr[i], L = NewPKV(kvs[L], nReduce), int64(L+1)%KvBufferSize }
    sort.Sort(arr)

    for i:=0; i<arr.Len(); i++ {
        _, err := wr[arr[i].Part].WriteString(arr[i].toStr()+"\n")
        if err != nil { panic(err.Error()) }
    }

    for _, w := range wr { w.Flush() }
    spill_round++
}

func KvBuffer(
    inCh chan KeyValue, //Get Key-Value pair.
    doneCh chan bool, //Output file name.
    nReduce int,
) {
    L, R, size, threshold := int64(0), int64(0), int64(0), int64(float64(KvBufferSize)*SpillRatio)
    buf := make([]KeyValue, KvBufferSize)

    mkdrErr := os.MkdirAll(MapDir, 0777)
    if mkdrErr != nil { panic(mkdrErr.Error()) }

    for kv := range inCh {
        buf[R], size, R = kv, size+1, (R+1)%KvBufferSize
        if size >= threshold {
            spill(buf, L, R, nReduce)
            L, size = R, 0
        }
    }

    if L != R { spill(buf, L, R, nReduce) }
    doneCh <- true
}
