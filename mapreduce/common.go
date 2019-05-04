package mapreduce

import (

)

const (
    INT64_MAX = int64((^uint64(0))>>1)
)

var (
    MapDir string = "./mapF_out/"
    ReduceDir string = "./reduceF_in/"
)

type KeyValue struct { Key, Value string }

func (kv *KeyValue) toStr() string { return kv.Key + " " + kv.Value }
