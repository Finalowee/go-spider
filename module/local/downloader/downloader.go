package downloader

import (
	"net/http"
	"go-spider/module"
	"go-spider/module/stub"
)

//下载器的实现类型
type myDownloader struct {
	//组件基础实例
	stub.ModuleInternal
	//下载用的HTTP客户端
	httpClient http.Client
}

func New(mid module.MID,
	client *http.Client,
	scoreCalculator module.CalculateScore) (module.Downloader, error) {
	moduleBase, err := stub.NewModuleInternal(mid, scoreCalculator)
	if nil != err {
		return nil, err
	}
	if nil == client {
		return nil, genParameterError("nil http client")
	}
	return &myDownloader{
		ModuleInternal: moduleBase,
		httpClient:     *client,
	}, nil
}

func (md *myDownloader) Download(req *module.Request) (*module.Response, error) {
	md.IncrHandlingNumber()
	defer md.DecrHandlingNumber()
	md.CalledCount()
	if nil == req {
		return nil, genParameterError("nil request")
	}
	httpReq := req.HTTPReq()
	if nil == httpReq {
		return nil, genParameterError("nil HTTP request")
	}
	httpResp, err := md.httpClient.Do(httpReq)
	md.IncrAcceptedCount()
	// 日志
	if nil != err {
		return nil, err
	}
	md.IncrCompletedCount()
	return module.NewResponse(httpResp, req.Depth()), nil
}
