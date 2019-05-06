package mapreduce

import (

)

//ThrdPool to control the num of executing thread.
type ThrdPool struct {
    MaxThrd int //Number of max executing thread.
    NewThrdCh chan func(chan bool) //Channel to send new thread.
    DoneCh chan bool //Channel to send task done signal after NewThrdCh is closed.
    numExecuting int //Numer of executing thread.
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
        innerThrdCh: make(chan bool, MaxThrd),
    }
    return &ret
}


/*Use go ThrdPool.Run() to call.

Send thread by ThrdPool.NewThrdCh.

Close ThrdPool.NewThrdCh when no more trhead to go.

After existing thread done and ThrdPool.NewThrdCh closed, send true to DoneCh.*/
func (pool *ThrdPool) Run() {
    for thrdInput := true; thrdInput == true; {
        if pool.numExecuting == pool.MaxThrd {
            <-pool.innerThrdCh
            pool.numExecuting--
        } else if pool.numExecuting < pool.MaxThrd {
            select {
            case <-pool.innerThrdCh:
                pool.numExecuting--
            case thrd, ok := <-pool.NewThrdCh:
                if thrd != nil {
                    pool.numExecuting++
                    go thrd(pool.innerThrdCh)
                }
                if ok == false { thrdInput = false }
            }
        } else {
            panic("Thread pool runing thread more than limits")
        }
    }

    for pool.numExecuting > 0 {
        <-pool.innerThrdCh
        pool.numExecuting--
    }

    pool.DoneCh <- true
}
