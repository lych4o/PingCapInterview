package mapreduce

import (
    //"fmt"
)

type thrdPool struct {
    numThrd uint32
    numExecuting uint32
    waitList []func()
    newThrdCh chan func()
    doneCh chan bool
    innerThrdCh []chan bool
    innerChStack []uint32
}

func newThrdPool(
    numThrd uint32,
    newThrdCh chan func(),
    doneCh chan bool,
    ) *thrdPool {
    ret := thrdPool{
        numThrd: numThrd,
        numExecuting: 0,
        newThrdCh: newThrdCh,
        doneCh: doneCh,
        waitList: make([]func(), numThrd*2),
        innerThrdCh: make([]chan bool, numThrd),
        innerChStack: make([]uint32, numThrd),
    }
    for i := uint32(0); i < numThrd; i++ {
        ret.innerChStack[i] = i
    }
    return &ret
}
