package buffer

import (
	"fmt"
	"go-spider/errors"
	"sync"
	"sync/atomic"
)

//数据缓冲池的接口类型
type Pool interface {
	//用于获取池中缓冲器的统一容量
	BufferCap() uint32
	//用于获取池中缓冲器的最大数量
	MaxBufferNumber() uint32
	//用于获取池中缓冲器的数量
	BufferNumber() uint32
	//用于获取缓冲池中数据的总数
	Total() uint64
	// Put用于向缓冲池放入数据。
	//注意！本方法是阻寒的。
	//若缓冲池已关闭，则会直接返回非 nil 的错误值
	Put(datum interface{}) error
	// Get用于从缓冲池获取数据。
	//注意！本方法是阻塞的。
	//若缓冲池已关闭，则会直接返回非nil的错误值
	Get() (datum interface{}, err error)
	// Close用于关闭缓冲池。
	//若缓冲池之前已关闭则返回false,否则返回true
	Close() bool
	//用于判断缓冲池是否已关闭
	Closed() bool
}

//数据缓冲池接口的实现类型
type myPool struct {
	//缓冲器的统一容量
	bufferCap uint32
	//缓冲器的最大数量
	maxBufferNumber uint32
	//缓冲器的实际数量
	bufferNumber uint32
	//池中数据的总数
	total uint64
	//存放缓冲器的通道
	bufCh chan Buffer
	//缓冲池的关闭状态：0-未关闭；1-已关闭
	closed uint32
	//保护内部共享资源的读写锁
	rwlock sync.RWMutex
}

// NewPool 用于创建一个数据缓冲池。
// 参数bufferCap代表池内缓冲器的统一容量。
// 参数maxBufferNumber代表池中最多包含的缓冲器的数量。
func NewPool(
	bufferCap uint32,
	maxBufferNumber uint32) (Pool, error) {
	if bufferCap == 0 {
		errMsg := fmt.Sprintf("illegal buffer cap for buffer pool: %d", bufferCap)
		return nil, errors.NewIllegalParameterError(errMsg)
	}
	if maxBufferNumber == 0 {
		errMsg := fmt.Sprintf("illegal max buffer number for buffer pool: %d", maxBufferNumber)
		return nil, errors.NewIllegalParameterError(errMsg)
	}
	bufCh := make(chan Buffer, maxBufferNumber)
	buf, _ := NewBuffer(bufferCap)
	bufCh <- buf
	return &myPool{
		bufferCap:       bufferCap,
		maxBufferNumber: maxBufferNumber,
		bufferNumber:    1,
		bufCh:           bufCh,
	}, nil
}
func (mp *myPool) BufferCap() uint32 {
	return uint32(cap(mp.bufCh))
}

func (mp *myPool) MaxBufferNumber() uint32 {
	return mp.maxBufferNumber
}

func (mp *myPool) BufferNumber() uint32 {
	return atomic.LoadUint32(&mp.bufferNumber)
}

func (mp *myPool) Total() uint64 {
	return atomic.LoadUint64(&mp.total)
}

func (mp *myPool) Close() bool {
	if !atomic.CompareAndSwapUint32(&mp.closed, 0, 1) {
		return false
	}
	mp.rwlock.RLock()
	defer mp.rwlock.RUnlock()
	close(mp.bufCh)
	for buf := range mp.bufCh {
		buf.Close()
	}
	return true
}

func (mp *myPool) Closed() bool {
	return atomic.LoadUint32(&mp.closed) == 1
}

func (mp *myPool) Put(datum interface{}) (err error) {
	if mp.Closed() {
		return ErrClosedBufferPool
	}
	var cnt uint32
	maxCnt := mp.BufferNumber() * 5
	var ok bool
	for buf := range mp.bufCh {
		ok, err = mp.putData(buf, datum, &cnt, maxCnt)
		if ok || nil != err {
			break
		}
	}
	return
}

func (mp *myPool) putData(buf Buffer, datum interface{}, count *uint32, maxCount uint32) (ok bool, err error) {
	if mp.Closed() {
		return false, ErrClosedBufferPool
	}
	defer func() {
		mp.rwlock.RLock()
		if mp.Closed() {
			// 缓冲器减一
			atomic.AddUint32(&mp.bufferNumber, ^uint32(0))
			err = ErrClosedBufferPool
		} else {
			mp.bufCh <- buf
		}
		mp.rwlock.RUnlock()
	}()
	ok, err = buf.Put(datum)
	if ok {
		atomic.AddUint64(&mp.total, 1)
		return
	}
	if nil != err {
		return
	}
	*count++
	// 动态扩充缓冲器
	if *count >= maxCount && mp.BufferNumber() < mp.MaxBufferNumber() {
		mp.rwlock.Lock()
		if mp.BufferNumber() < mp.MaxBufferNumber() {
			if mp.Closed() {
				mp.rwlock.Unlock()
				return
			}
			newBuf, _ := NewBuffer(mp.BufferCap())
			newBuf.Put(datum)
			mp.bufCh <- newBuf
			atomic.AddUint32(&mp.bufferNumber, 1)
			atomic.AddUint64(&mp.total, 1)
			ok = true
		}
		mp.rwlock.Unlock()
		*count = 0
	}
	return
}

func (mp *myPool) Get() (datum interface{}, err error) {
	if mp.Closed() {
		return nil, ErrClosedBufferPool
	}
	var cnt uint32
	maxCnt := mp.BufferNumber() * 10
	for buf := range mp.bufCh {
		datum, err = mp.getData(buf, &cnt, maxCnt)
		if datum != nil || err != nil {
			break
		}
	}
	return
}

func (mp *myPool) getData(buf Buffer, cnt *uint32, maxCnt uint32) (datum interface{}, err error) {
	if mp.Closed() {
		return nil, ErrClosedBufferPool
	}
	defer func() {
		// 关闭空闲缓冲器
		if *cnt >= maxCnt && buf.Len() == 0 && mp.BufferNumber() > 1 {
			buf.Close()
			atomic.AddUint32(&mp.bufferNumber, ^uint32(0))
			*cnt = 0
			return
		}
		// 归还缓冲器
		mp.rwlock.RLock()
		if mp.Closed() {
			atomic.AddUint32(&mp.bufferNumber, ^uint32(0))
			err = ErrClosedBufferPool
		} else {
			mp.bufCh <- buf
		}
		mp.rwlock.RUnlock()
	}()
	datum, err = buf.Get()
	if datum != nil {
		atomic.AddUint64(&mp.total, ^uint64(0))
		return
	}
	if nil != err {
		return
	}
	*cnt++
	return
}
