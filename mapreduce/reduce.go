package mapreduce

import (
    "strconv"
    //"fmt"
)

func ExampleReduceF(Key string, Values []string) string {
    if len(Values) != 1 { return strconv.FormatInt(INT64_MAX, 10) }
    return Values[0]
}
