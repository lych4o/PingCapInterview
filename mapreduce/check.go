package mapreduce

import (
    "strings"
    "io"
    "bufio"
    "strconv"
    "sort"
    "os"
)

type sa []string
func (a sa) Len() int { return len(a) }
func (a sa) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sa) Less(i, j int) bool { return a[i] < a[j] }

//Checker
func Check(fileName string) string {
    arr := make(sa, 10000)[:0]
    f, err := os.Open(fileName)
    if err != nil { panic(err.Error()) }
    defer f.Close()
    rd := bufio.NewReader(f)

    for i:=1; ;i++{
        line, _, rdErr := rd.ReadLine()
        if rdErr == io.EOF { break }
        arr = append(arr, string(line)+" "+strconv.FormatInt(int64(i), 10))
    }

    sort.Sort(arr)

    ans, min := "", int64(INT64_MAX)
    for i,j:=0,0; i<arr.Len(); i=j {
        kv := strings.Split(arr[i], " ")
        k, v := kv[0], kv[1]
        for j=i+1; j<arr.Len(); j++ {
            jkv := strings.Split(arr[j], " ")
            jk, _ := jkv[0], jkv[1]
            if jk != k { break }
        }
        if j>i+1 { continue }
        idx, parseErr := strconv.ParseInt(v, 10, 64)
        if parseErr != nil { panic(parseErr.Error()) }
        if idx < min { ans, min = k, idx }
    }

    return ans
}

