package buffer

import (
	"fmt"
	"spider/errors"
	"sync"
	"sync/atomic"
)

// FIFO的缓冲器的接口类型
type Buffer interface {
	//用于获取本缓冲器的容量
	Cap() uint32
	//用于获取本缓冲器中数据的数量
	Len() uint32
	// Put用于向缓冲器放入数据。
	//注意！本方法是非阻寒的。
	//若缓冲器已关闭，则会直接返回非nil的错误值
	Put(datum interface{}) (bool, error)
	// Get用于从缓冲器获取器。
	//注意！本方法是非阻塞的。
	//若缓冲器已关闭，则会直接返回非nil的错误值
	Get() (interface{}, error)
	// Close用于关闭缓冲器。
	//若缓冲器之前已关闭，则返回false,否则返回true
	Close() bool
	//用于判断缓冲器是否已关闭
	Closed() bool
}

//集冲器接口的实现类型
type myBuffer struct {
	//存放数据的通道
	ch chan interface{}
	//缓冲器的关闭状态：0-未关闭；2-已关闭
	closed uint32
	//为了消除因关闭缓冲器而产生的竞态条件的读写锁
	closingLock sync.RWMutex
}

func (mb *myBuffer) Cap() uint32 {
	return uint32(cap(mb.ch))
}

func (mb *myBuffer) Len() uint32 {
	return uint32(len(mb.ch))
}

func (mb *myBuffer) Put(datum interface{}) (ok bool, err error) {
	mb.closingLock.RLock()
	defer mb.closingLock.RUnlock()
	if mb.Closed() {
		return false, ErrClosedBuffer
	}
	select {
	case mb.ch <- datum:
		ok = true
	default:
		ok = false
	}
	return
}

func (mb *myBuffer) Get() (interface{}, error) {
	select {
	case datum, ok := <-mb.ch:
		if !ok {
			return nil, ErrClosedBuffer
		}
		return datum, nil
	default:
		return nil, nil
	}
}

func (mb *myBuffer) Close() bool {
	if atomic.CompareAndSwapUint32(&mb.closed, 0, 1) {
		mb.closingLock.Lock()
		close(mb.ch)
		mb.closingLock.Unlock()
		return true
	}
	return false
}

func (mb *myBuffer) Closed() bool {
	if atomic.LoadUint32(&mb.closed) == 0 {
		return false
	}
	return true
}

func NewBuffer(size uint32) (Buffer, error) {
	if 0 == size {
		errMsg := fmt.Sprintf("illegal size for buffer: %d", size)
		return nil, errors.NewIllegalParameterError(errMsg)
	}
	return &myBuffer{}, nil
}
