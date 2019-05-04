package mapreduce

import (
    "strconv"
    //"fmt"
)

func ExampleReduceF(Key string, Values []string) string {
    //fmt.Printf("Reduce(%v, %v)\n",Key, Values)
    if len(Values) != 1 { return strconv.FormatInt(INT64_MAX, 10) }
    //fmt.Printf("Single word %v, first shown in %v\n", Key, Values[0])
    return Values[0]
}
