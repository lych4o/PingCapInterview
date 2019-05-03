package mapreduce

import (
    //"fmt"
    //"time"
)

type emptyStackError struct {}
func (err *emptyStackError) Error() string {
    return "Call stack.pop() when queue is empty"
}
type FStack struct { stk []func(chan bool) }
func NewFStack() *FStack {
    ret := &FStack{}
    ret.stk = make([]func(chan bool), 1000)
    ret.stk = ret.stk[:0]
    return ret
}
func (s *FStack) Size() int { return len(s.stk) }
func (s *FStack) Empty() bool { return s.Size() == 0 }
func (s *FStack) Push(f func(chan bool)) { s.stk = append(s.stk, f) }
func (s *FStack) Top() func(chan bool) { return s.stk[len(s.stk)-1] }
func (s *FStack) Pop() (func(chan bool), error) {
    if s.Empty() { return nil, &emptyStackError{} }
    ret := s.Top()
    s.stk = s.stk[:len(s.stk)-1]
    return ret, nil
}

type ThrdPool struct {
    numThrd int
    numExecuting int
    thrdStack *FStack
    newThrdCh chan func(chan bool)
    doneCh chan bool
    innerThrdCh chan bool
}

func NewThrdPool(
    numThrd int,
    newThrdCh chan func(chan bool),
    doneCh chan bool,
    ) *ThrdPool {
    ret := ThrdPool{
        numThrd: numThrd,
        numExecuting: 0,
        newThrdCh: newThrdCh,
        doneCh: doneCh,
        thrdStack: NewFStack(),
        innerThrdCh: make(chan bool),
    }
    return &ret
}

func (pool *ThrdPool) runThrd() {
    for pool.numExecuting < pool.numThrd && !pool.thrdStack.Empty() {
        pool.numExecuting++
        f, err := pool.thrdStack.Pop()
        if err != nil {
            //TODO
        }
        go f(pool.innerThrdCh)
    }
}

func (pool *ThrdPool) Run() {
    //Use go to call
    for thrdInput := true; thrdInput == true; {
        select {
            case <-pool.innerThrdCh:
                pool.numExecuting--
                pool.runThrd()
            case f, ok := <-pool.newThrdCh:
                if f != nil { pool.thrdStack.Push(f) }
                pool.runThrd()
                if ok == false { thrdInput = false }
        }
    }
    for pool.numExecuting > 0 || !pool.thrdStack.Empty() {
        _ = <-pool.innerThrdCh
        pool.numExecuting--
        pool.runThrd()
    }
    pool.doneCh <- true
}
