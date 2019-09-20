package analyzer

import (
	"fmt"
	"go-spider/module"
	"go-spider/module/stub"
	"go-spider/toolkit/reader"
)

type myAnalyzer struct {
	stub.ModuleInternal
	respParsers []module.ParseResponse
}

func (ma *myAnalyzer) RespParsers() []module.ParseResponse {
	parsers := make([]module.ParseResponse, len(ma.respParsers))
	copy(parsers, ma.respParsers)
	return parsers
}

func (ma *myAnalyzer) Analyze(resp *module.Response) (data []module.Data, errs []error) {
	ma.IncrHandlingNumber()
	defer ma.DecrHandlingNumber()
	ma.IncrCalledCount()
	if nil == resp {
		errs = append(errs, genParameterError("nil response"))
		return
	}
	httpResp := resp.HTTPResp()
	if nil == httpResp {
		errs = append(errs, genParameterError("nil HTTP response"))
		return
	}
	httpReq := httpResp.Request
	if nil == httpReq {
		errs = append(errs, genParameterError("nil HTTP request"))
		return
	}
	var reqUrl = httpReq.URL
	if nil == reqUrl {
		errs = append(errs, genParameterError("nil HTTP request URL"))
		return
	}
	ma.IncrAcceptedCount()
	respDepth := resp.Depth()

	if nil != httpResp.Body {
		defer httpResp.Body.Close()
	}
	mr, err := reader.NewMultipleReader(httpResp.Body)
	if nil != err {
		errs = append(errs, genError(err.Error()))
		return
	}
	data = []module.Data{}
	for _, respParser := range ma.respParsers {
		httpResp.Body = mr.Reader()
		pData, pErr := respParser(httpResp, respDepth)
		if pData != nil {
			for _, pd := range pData {
				if nil == pd {
					continue
				}
				data = appendDataList(data, pd, respDepth)
			}
		}
		if pErr != nil {
			for _, pe := range pErr {
				if nil == pe {
					continue
				}
				errs = append(errs, pe)
			}
		}
	}
	if 0 == len(errs) {
		ma.IncrCompletedCount()
	}
	return data, errs
}

func New(mid module.MID,
	respParsers []module.ParseResponse,
	scoreCalculator module.CalculateScore) (module.Analyzer, error) {
	moduleBase, err := stub.NewModuleInternal(mid, scoreCalculator)
	if nil != err {
		return nil, err
	}
	if nil == respParsers {
		return nil, genParameterError("nil response parses")
	}
	if len(respParsers) == 0 {
		return nil, genParameterError("empty response parser list")
	}
	var innerParsers []module.ParseResponse
	for i, parser := range respParsers {
		if nil == parser {
			return nil, genParameterError(fmt.Sprintf("nil response parser[%d]", i))
		}
		innerParsers = append(innerParsers, parser)
	}
	return &myAnalyzer{
		ModuleInternal: moduleBase,
		respParsers:    innerParsers,
	}, nil
}

// appendDataList 用于添加请求值或条目值到列表。
func appendDataList(dataList []module.Data, data module.Data, respDepth uint32) []module.Data {
	if data == nil {
		return dataList
	}
	req, ok := data.(*module.Request)
	if !ok {
		return append(dataList, data)
	}
	newDepth := respDepth + 1
	if req.Depth() != newDepth {
		req = module.NewRequest(req.HTTPReq(), newDepth)
	}
	return append(dataList, req)
}
