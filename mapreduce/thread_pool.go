package mapreduce

import (
    //"fmt"
    //"time"
)


//Raise when call FStack.Pop() while FStack is empty.
type EmptyFStackError struct {}

//Return string.
func (err *EmptyFStackError) Error() string {
    return "Call FStack.Pop() when queue is empty"
}

//Stack of func(chan bool).
type FStack struct { Stk []func(chan bool) }

//Initial cap(FStack.Stk)
var StackInitSize int = 1024

//Return a new FStack.
func NewFStack() *FStack {
    ret := &FStack{}
    ret.Stk = make([]func(chan bool), StackInitSize)
    ret.Stk = ret.Stk[:0]
    return ret
}

//Return the size of FStack.
func (s *FStack) Size() int { return len(s.Stk) }

//Return true if FStack is empty, otherwise false.
func (s *FStack) Empty() bool { return s.Size() == 0 }

//Push a func(chan bool) to FStack.
func (s *FStack) Push(f func(chan bool)) { s.Stk = append(s.Stk, f) }

//Return the top func(chan bool) in FStack.
func (s *FStack) Top() func(chan bool) { return s.Stk[len(s.Stk)-1] }

/*If FStack not empty, return the top func(chan bool) in FStack and nil error.

Otherwise return nil func(chan bool) and an EmptyFStackError.*/
func (s *FStack) Pop() (func(chan bool), error) {
    if s.Empty() { return nil, &EmptyFStackError{} }
    ret := s.Top()
    s.Stk = s.Stk[:len(s.Stk)-1]
    return ret, nil
}

//ThrdPool to control the num of executing thread.
type ThrdPool struct {
    MaxThrd int //Number of max executing thread.
    NewThrdCh chan func(chan bool) //Channel to send new thread.
    DoneCh chan bool //Channel to send task done signal after NewThrdCh is closed.
    numExecuting int //Numer of executing thread.
    thrdStack *FStack //Store ready thread to go.
    innerThrdCh chan bool //Inner channel to get done signal from executing thread.
}

//Return a new ThrdPool with input MaxThrd, NewThrdCh, DoneCh.
func NewThrdPool(
    MaxThrd int,
    NewThrdCh chan func(chan bool),
    DoneCh chan bool,
    ) *ThrdPool {
    ret := ThrdPool{
        MaxThrd: MaxThrd,
        numExecuting: 0,
        NewThrdCh: NewThrdCh,
        DoneCh: DoneCh,
        thrdStack: NewFStack(),
        innerThrdCh: make(chan bool, MaxThrd),
    }
    return &ret
}

//Try to run thread in stack as much as possible.
func (pool *ThrdPool) runThrd() {
    for pool.numExecuting < pool.MaxThrd && !pool.thrdStack.Empty() {
        pool.numExecuting++
        f, err := pool.thrdStack.Pop()
        if err != nil { panic(err.Error()) }
        go f(pool.innerThrdCh)
    }
}

/*Use go ThrdPool.Run() to call.

Send thread by ThrdPool.NewThrdCh.

Close ThrdPool.NewThrdCh when no more trhead to go.

After existing thread done and ThrdPool.NewThrdCh closed, send true to DoneCh.*/
func (pool *ThrdPool) Run() {
    for thrdInput := true; thrdInput == true; {
        select {
            case <-pool.innerThrdCh:
                pool.numExecuting--
                pool.runThrd()
            case f, ok := <-pool.NewThrdCh:
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
    pool.DoneCh <- true
}
