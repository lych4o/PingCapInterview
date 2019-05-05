package mapreduce

import (
    "testing"
    //"os"
    //"math/rand"
    "fmt"
    "time"
    //"container/heap"
)

type testError struct { info string }
func newTestError(info string) *testError { return &testError{info} }
func (err *testError) Error() string { return err.info }

//Single test with input file of n words whose length is wordLen, containing first-characterNum english characters. testId is trival, only to print, any integer is ok.
func SingleTest(n int, wordLen int, characterNum int, testId int) error {
    t1 := time.Now()
    fileGen(n, wordLen, characterNum)
    file_gen_time := time.Since(t1)
    fmt.Printf("file generator time: %v\n", file_gen_time)

    t1 = time.Now()
    ans1 := RunExample(InputFileName, 4)
    mr_time := time.Since(t1)
    fmt.Printf("mr_time: %v\n", mr_time)

    t1 = time.Now()
    _ = RunExample(InputFileName, 8)
    mr1_time := time.Since(t1)
    fmt.Printf("mr1_time: %v\n", mr1_time)

    t1 = time.Now()
    ans2 := Check(InputFileName)
    check_time := time.Since(t1)
    fmt.Printf("check_time: %v\n", check_time)

    if ans1 != ans2 { return newTestError(string("Wrong Answer! mr_output:"+ans1+", checker_output:"+ans2))
    } else {
        fmt.Printf("Pass Test %v\n\n", testId)
        return nil
    }
}

/**********************
Modify this function to have a different test
**********************/
func TestRunExample(t *testing.T) {

    //Change variable there to control test
    var (
        testCaseNum int = 5
        wordNum int = 10000000
        wordLen int = 10
        characterNum int = 10
    )

    for i:=0; i<testCaseNum; i++ {
        ok := SingleTest(wordNum, wordLen, characterNum, i+1)
        if ok != nil {
            t.Error(ok.Error())
        }
    }
}
