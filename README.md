# Install to local $GOPATH
`go get github.com/lych4o/PingCapInterview/mapreduce`

# Built-in test in shell
```
cd $GOPATH/src/github.com/lych4o/PingCapInterview/mapreduce
go test -v
```
Change TestRunExample() in mr_test.go to modify test.

# How to use in go
```
import (
    mr "github.com/lych4o/PingCapInterview/mapreduce"
)
func main(){
    var first_unique_word string = mr.RunExample( InputFileName, nReduce )
}
```
