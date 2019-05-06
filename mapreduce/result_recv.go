package mapreduce

import (
    "strconv"
)

//Get reduce result to decide First unique word.
func resultRecv(
    resultCh chan string,
    returnCh chan string,
) {
    min := int64(INT64_MAX)
    for recv := range resultCh{
        idx, parseErr := strconv.ParseInt(recv, 10, 64)
        if parseErr != nil { panic(parseErr.Error()) }
        if idx < min { min = idx }
    }
    if min == int64(INT64_MAX) { returnCh <- strconv.FormatInt(0, 10)
    } else { returnCh <- strconv.FormatInt(min, 10) }
}
