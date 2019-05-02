package mapreduce

import (
    //"fmt"
    //"time"
)

type fStack struct {
    stk []func(chan bool)
}

type emptyStackError struct {}
func (err *emptyStackError) Error() string {
    return "Call stack.pop() when queue is empty"
}
func (s *fStack) size() int { return len(s.stk) }
func (s *fStack) empty() bool { return s.size() == 0 }
func (s *fStack) push(f func(chan bool)) { s.stk = append(s.stk, f) }
func (s *fStack) top() func(chan bool) { return s.stk[len(s.stk)-1] }
func (s *fStack) pop() (func(chan bool), error) {
    if len(s.stk) == 0 { return nil, &emptyStackError{} }
    ret := s.top()
    s.stk = s.stk[:len(s.stk)-1]
    return ret, nil
}

type thrdPool struct {
    numThrd int
    numExecuting int
    thrdStack fStack
    newThrdCh chan func(chan bool)
    doneCh chan bool
    innerThrdCh chan bool
}

func newThrdPool(
    numThrd int,
    newThrdCh chan func(chan bool),
    doneCh chan bool,
    ) *thrdPool {
    ret := thrdPool{
        numThrd: numThrd,
        numExecuting: 0,
        newThrdCh: newThrdCh,
        doneCh: doneCh,
        thrdStack: fStack{},
        innerThrdCh: make(chan bool),
    }
    return &ret
}

func (pool *thrdPool) runThrd() {
    for pool.numExecuting < pool.numThrd && !pool.thrdStack.empty() {
        f, err := pool.thrdStack.pop()
        if err != nil {
            //TODO
        }
        go f(pool.innerThrdCh)
    }
}

func (pool *thrdPool) run() {
    //Use go to call
    for thrdInput := true; thrdInput == true; {
        select {
            case <-pool.innerThrdCh:
                pool.numExecuting--
                pool.runThrd()
            case f, ok := <-pool.newThrdCh:
                pool.thrdStack.push(f)
                pool.runThrd()
                if ok == false { thrdInput = false }
        }
    }
    for pool.numExecuting > 0 && !pool.thrdStack.empty() {
        _ = <-pool.innerThrdCh
        pool.runThrd()
    }
    pool.doneCh <- true
}
