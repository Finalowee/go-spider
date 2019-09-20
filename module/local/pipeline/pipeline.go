package pipeline

import (
	"fmt"
	"go-spider/helper/log"
	"go-spider/module"
	"go-spider/module/stub"
)

var logger = log.DLogger()

type myPipeline struct {
	stub.ModuleInternal
	itemProcessors []module.ProcessItem
	failFast       bool
}

func (mp *myPipeline) ItemProcessors() []module.ProcessItem {
	processors := make([]module.ProcessItem, len(mp.itemProcessors))
	copy(processors, mp.itemProcessors)
	return processors
}

func (mp *myPipeline) FailFast() bool {
	return mp.failFast
}

func (mp *myPipeline) SetFailFast(failFast bool) {
	mp.failFast = failFast
}

func New(
	mid module.MID,
	itemProcessors []module.ProcessItem,
	scoreCalculator module.CalculateScore) (module.Pipeline, error) {
	moduleBase, err := stub.NewModuleInternal(mid, scoreCalculator)
	if nil != err {
		return nil, err
	}
	if nil == itemProcessors {
		return nil, nil
	}
	if len(itemProcessors) == 0 {
		return nil, genParameterError("empty item processor list")
	}
	var innerProcessors []module.ProcessItem
	for i, pipe := range itemProcessors {
		if nil == pipe {
			err := genParameterError(fmt.Sprintf("nil item processor[%d]", i))
			return nil, err
		}
		innerProcessors = append(innerProcessors, pipe)
	}
	return &myPipeline{
		ModuleInternal: moduleBase,
		itemProcessors: innerProcessors,
	}, nil
}

func (mp *myPipeline) Send(item module.Item) []error {
	mp.IncrHandlingNumber()
	defer mp.DecrHandlingNumber()
	mp.IncrCalledCount()
	var errs []error
	if item == nil {
		err := genParameterError("nil item")
		errs = append(errs, err)
		return errs
	}
	mp.IncrAcceptedCount()
	var curItem = item
	for _, processor := range mp.itemProcessors {
		processedItem, err := processor(curItem)
		if nil != err {
			errs = append(errs, err)
			if mp.failFast {
				break
			}
		}
		if nil != processedItem {
			curItem = processedItem
		}
	}
	if len(errs) == 0 {
		mp.IncrCompletedCount()
	}
	return errs
}

type extraSummaryStruct struct {
	FailFast        bool `json:"fail_fast"`
	ProcessorNumber int  `json:"processor_number"`
}

func (mp *myPipeline) Summary() module.SummaryStruct {
	summary := mp.ModuleInternal.Summary()
	summary.Extra = extraSummaryStruct{
		FailFast:        mp.failFast,
		ProcessorNumber: len(mp.itemProcessors),
	}
	return summary
}
