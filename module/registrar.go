package module

import (
	"fmt"
	"go-spider/errors"
	"sync"
)

//组件注册器的接口
type Registrar interface {
	//用于注册组件实例
	Register(module Module) (bool, error)
	//用于注销组件实例
	Unregister(mid MID) (bool, error)
	//用于获取一个指定类型的组件的实例，
	//该函数基于负载均衡策略返回实例
	Get(moduleType Type) (Module, error)
	//用于获取指定类型的所有组件实例
	GetAllByType(moduleType Type) (map[MID]Module, error)
	//用于获取所有组件实例
	GetAll() map[MID]Module
	//清除所有的组件注册记录
	Clear()
}

type myRegistrar struct {
	moduleTypeMap map[Type]map[MID]Module
	rwlock        sync.RWMutex
}

//用于创建一个组件注册器的实例
func NewRegistrar() Registrar {
	return &myRegistrar{
		moduleTypeMap: map[Type]map[MID]Module{},
	}
}

func (mr *myRegistrar) Register(module Module) (bool, error) {
	if nil == module {
		return false, errors.NewIllegalParameterError("nil module instance")
	}
	mid := module.ID()
	parts, err := SplitMID(mid)
	if nil != err {
		return false, err
	}
	moduleType := legalLetterTypeMap[parts[0]]
	if !CheckType(moduleType, module) {
		errMsg := fmt.Sprintf("incorrect module type: %s", moduleType)
		return false, errors.NewIllegalParameterError(errMsg)
	}
	mr.rwlock.Lock()
	defer mr.rwlock.Unlock()
	modules := mr.moduleTypeMap[moduleType]
	if nil == modules {
		modules = map[MID]Module{}
	}
	if _, ok := modules[mid]; ok {
		return false, nil
	}
	modules[mid] = module
	mr.moduleTypeMap[moduleType] = modules
	return true, nil
}

func (mr *myRegistrar) Unregister(mid MID) (bool, error) {
	parts, err := SplitMID(mid)
	if nil != err {
		return false, err
	}
	moduleType := legalLetterTypeMap[parts[0]]
	var deleted bool
	mr.rwlock.Lock()
	defer mr.rwlock.Unlock()
	if modules, ok := mr.moduleTypeMap[moduleType]; ok {
		if _, ok := modules[mid]; ok {
			delete(modules, mid)
			deleted = true
		}
	}
	return deleted, nil
}

func (mr *myRegistrar) Get(moduleType Type) (Module, error) {
	modules, err := mr.GetAllByType(moduleType)
	if nil != err {
		return nil, err
	}
	minScore := uint64(0)
	var selectedModule Module
	for _, module := range modules {
		SetScore(module)
		score := module.Score()
		if minScore == 0 || score < minScore {
			selectedModule = module
			minScore = score
		}
	}
	return selectedModule, nil
}

func (mr *myRegistrar) GetAllByType(moduleType Type) (map[MID]Module, error) {
	if !LegalType(moduleType) {
		errMsg := fmt.Sprintf("illegal module type: %s", moduleType)
		return nil, errors.NewIllegalParameterError(errMsg)
	}
	mr.rwlock.RLock()
	defer mr.rwlock.RUnlock()
	modules := mr.moduleTypeMap[moduleType]
	if len(modules) == 0 {
		return nil, ErrNotFoundModuleInstance
	}
	result := map[MID]Module{}
	for mid, module := range modules {
		result[mid] = module
	}
	return result, nil
}

func (mr *myRegistrar) GetAll() map[MID]Module {
	result := map[MID]Module{}
	mr.rwlock.RLock()
	defer mr.rwlock.RUnlock()
	for _, modules := range mr.moduleTypeMap {
		for mid, module := range modules {
			result[mid] = module
		}
	}
	return result
}

func (mr *myRegistrar) Clear() {
	mr.rwlock.Lock()
	defer mr.rwlock.Unlock()
	mr.moduleTypeMap = map[Type]map[MID]Module{}
}
