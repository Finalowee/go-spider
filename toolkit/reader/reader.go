package reader

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
)

//多重读取器的接口
type MultipleReader interface {
	// Reader用于获取一个可关闭读取器的实例。
	//后者会持有该多重读取器中的数据
	Reader() io.ReadCloser
}

type myMultipleReader struct {
	data []byte
}

func NewMultipleReader(r io.Reader) (MultipleReader, error) {
	var data []byte
	var err error
	if nil != r {
		data, err = ioutil.ReadAll(r)
		if nil != err {
			return nil, fmt.Errorf("multipie reader: couldn't create a new one: %s", err)
		}
	} else {
		data = []byte{}
	}
	return &myMultipleReader{data: data}, nil
}

func (mr *myMultipleReader) Reader() io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader(mr.data))
}
