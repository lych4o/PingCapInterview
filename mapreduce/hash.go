package mapreduce

import (
    "hash/fnv"
)
func MHash(str string, mod int) int {
    hash32 := fnv.New32()
    _, err := hash32.Write([]byte(str))
    if err != nil {
        //TODO
    }
    hashCode := hash32.Sum(nil)
    var ret int = 0
    for i := 0; i < 4; i++ {
        ret = (ret << 8) | int(hashCode[i])
    }
    return ret % mod
}
