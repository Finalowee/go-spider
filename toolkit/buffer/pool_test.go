package buffer

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func TestNewBuffer(t *testing.T) {
	fmt.Println(^uint32(0))
	var i uint32 = 90
	atomic.AddUint32(&i, ^uint32(0))
	fmt.Println(^uint32(0))
	fmt.Println(atomic.LoadUint32(&i))

}
