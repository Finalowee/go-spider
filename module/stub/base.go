package stub

import "go-spider/module"

type ModuleInternal interface {
	module.Module
	// 调用次数加一
	IncrCalledCount()
	// 接受次数加一
	IncrAcceptedCount()
	// 完成次数加一
	IncrCompletedCount()
	// 处理数加一
	IncrHandlingNumber()
	// 处理数减一
	DecrHandlingNumber()
	Clear()
}
