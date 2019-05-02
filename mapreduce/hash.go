package mapreduce

import (
    "hash/fnv"
)
func MHash(str string, mod uint32) uint32 {
    hash32 := fnv.New32()
    _, err := hash32.Write([]byte(str))
    if err != nil {
        //TODO
    }
    hashCode := hash32.Sum(nil)
    var ret uint32 = 0
    for i := 0; i < 4; i++ {
        ret = (ret << 8) | uint32(hashCode[i])
    }
    return ret % mod
}
