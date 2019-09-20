package module

import "net/http"

type Request struct {
	httpReq *http.Request
	depth   uint32
}

//用于创建一个新的请求实例
func NewRequest(httpReq *http.Request, depth uint32) *Request {
	return &Request{httpReq: httpReq, depth: depth}
}

//用于获取 HTTP 请求
func (req *Request) HTTPReq() *http.Request {
	return req.httpReq
}

//用于获取请求的深度
func (req *Request) Depth() uint32 {
	return req.depth
}

//数据响应的类型
type Response struct {
	// HTTP响应
	httpResp *http.Response
	//响应的深度
	depth uint32
}

//用于创建一个新的响应实例
func NewResponse(httpResp *http.Response, depth uint32) *Response {
	return &Response{httpResp: httpResp, depth: depth}
}

//用于获取HTTP响应
func (resp *Response) HTTPResp() *http.Response {
	return resp.httpResp
}

//用于获取响应深度
func (resp *Response) Depth() uint32 {
	return resp.depth
}

//条目的类型
type Item map[string]interface{}

//数据的接口类型
type Data interface {
	//用于判断数据是否有效
	Valid() bool
}

//用于判断请求是否有效
func (req *Request) Valid() bool {
	return req.httpReq != nil && req.httpReq.URL != nil
}

//用于判断响应是否有效
func (resp *Response) Valid() bool {
	return resp.httpResp != nil && resp.httpResp.Body != nil
}

//用于判断条目是否有效
func (item Item) Valid() bool {
	return item != nil
}
