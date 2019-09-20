package scheduler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"go-spider/helper/cmap"
	"go-spider/helper/log"
	"go-spider/module"
	"go-spider/toolkit/buffer"
	"strings"
	"sync"
)

//调度器的接口类型
type Scheduler interface {
	// Init用于初始化调度器。
	//参数requestArgs代表请求相关的参数。
	//参数dataArgs代表数据相关的参数。
	//参数moduleArgs代表组件相关的参数
	Init(requestArgs RequestArgs, dataArgs DataArgs, moduleArgs ModuleArgs) (err error)
	// Start用于启动调度器并执行爬取流程
	//参数firstHTTPReq代表首次请求，调度器会以此为起始点开始执行爬取流程
	Start(firstHTTPReq *http.Request) (err error)
	//Stop用于停止调度器的运行
	//所有处理模块执行的流程都会被中止
	Stop() (err error)
	//用于获取调度器的状态
	Status() Status
	// ErrorChan用于获得错误通道。
	//调度器以及各个处理模块运行过程中出现的所有错误都会被发送到该通道。
	//若结果值为nil，则说明错误通道不可用或调度器已停止
	ErrorChan() <-chan error
	//用于判断所有处理模块是否都处于空闲状态
	Idle() bool
	//用于获取摘要实例
	Summary() SchedSummary
}

var logger = log.DLogger()

type myScheduler struct {
	maxDepth          uint32
	acceptedDomainMap cmap.ConcurrentMap
	registrar         module.Registrar
	reqBufferPool     buffer.Pool
	respBufferPool    buffer.Pool
	itemBufferPool    buffer.Pool
	errorBufferPool   buffer.Pool
	urlMap            cmap.ConcurrentMap // 已处理的地址
	ctx               context.Context
	cancelFunc        context.CancelFunc
	status            Status
	statusLock        sync.RWMutex
	summary           SchedSummary
}

// NewScheduler 会创建一个调度器实例。
func NewScheduler() Scheduler {
	return &myScheduler{}
}
func (ms *myScheduler) Init(requestArgs RequestArgs,
	dataArgs DataArgs, moduleArgs ModuleArgs) (err error) {
	logger.Info("Check Status form initialization")
	oldStatus, err := ms.checkAndSetStatus(SCHED_STATUS_INITIALIZING)
	if nil != err {
		return
	}
	defer func() {
		ms.statusLock.Lock()
		if nil != err {
			ms.status = oldStatus
		} else {
			ms.status = SCHED_STATUS_INITIALIZED
		}
		ms.statusLock.Unlock()
	}()
	logger.Info("Check request arguments...")
	if err = requestArgs.Check(); err != nil {
		return err
	}
	logger.Info("Request arguments are valid.")
	logger.Info("Check data arguments...")
	if err = dataArgs.Check(); err != nil {
		return err
	}
	logger.Info("Data arguments are valid.")
	logger.Info("Check module arguments...")
	if err = moduleArgs.Check(); err != nil {
		return err
	}
	logger.Info("Module arguments are valid.")
	logger.Info("Initialize scheduler’s fields...")
	if nil == ms.registrar {
		ms.registrar = module.NewRegistrar()
	} else {
		ms.registrar.Clear()
	}
	ms.maxDepth = requestArgs.MaxDepth
	logger.Infof("-- Max depth: %d", ms.maxDepth)
	ms.acceptedDomainMap, _ = cmap.NewConcurrentMap(1, nil)
	logger.Infof("-- URL map: length: %d, concurrency: %d", ms.acceptedDomainMap.Len(), ms.acceptedDomainMap.Concurrency())
	logger.Infof("-- Accepted primary domains: %v",
		requestArgs.AcceptedDomains)
	ms.urlMap, _ = cmap.NewConcurrentMap(16, nil)
	logger.Infof("-- URL map: length: %d, concurrency: %d",
		ms.urlMap.Len(), ms.urlMap.Concurrency())
	// 其他初始化

	ms.initBufferPool(dataArgs)
	ms.resetContext()
	ms.summary = newSchedSummary(requestArgs, dataArgs, moduleArgs, ms)

	logger.Info("Register modules...")
	if err = ms.registerModules(moduleArgs); err != nil {
		return err
	}
	logger.Info("Scheduler has been initialized.")
	return nil
}

func (ms *myScheduler) Start(req *http.Request) (err error) {
	defer func() {
		if p := recover(); nil != p {

			errMsg := fmt.Sprintf("Fatal scheduler error: %s", p)
			logger.Fatal(errMsg)
			err = genError(errMsg)
		}
	}()
	logger.Info("Start scheduler...")
	logger.Info("Check status for start...")
	var oldStatus Status
	oldStatus, err = ms.checkAndSetStatus(SCHED_STATUS_STARTING)
	defer func() {
		ms.statusLock.Lock()
		if err != nil {
			ms.status = oldStatus
		} else {
			ms.status = SCHED_STATUS_STARTED
		}
		ms.statusLock.Unlock()
	}()
	if err != nil {
		return
	}
	// 检查参数。
	logger.Info("Check first HTTP request...")
	if req == nil {
		err = genParameterError("nil first HTTP request")
		return
	}
	logger.Info("The first HTTP request is valid.")
	// 获得首次请求的主域名，并将其添加到可接受的主域名的字典。
	logger.Info("Get the primary domain...")
	logger.Infof("-- Host: %s", req.Host)
	var primaryDomain string
	primaryDomain, err = getPrimaryDomain(req.Host)
	if err != nil {
		return
	}
	logger.Infof("-- Primary domain: %s", primaryDomain)
	ms.acceptedDomainMap.Put(primaryDomain, struct{}{})
	// 开始调度数据和组件。
	if err = ms.checkBufferPoolForStart(); err != nil {
		return
	}
	ms.download()
	ms.analyze()
	ms.pick()
	logger.Info("Scheduler has been started.")
	// 放入第一个请求。
	firstReq := module.NewRequest(req, 0)
	ms.sendReq(firstReq)
	return nil
}

func (ms *myScheduler) Stop() (err error) {
	logger.Info("Stop scheduler...")
	// 检查状态。
	logger.Info("Check status for stop...")
	var oldStatus Status
	oldStatus, err = ms.checkAndSetStatus(SCHED_STATUS_STOPPING)
	defer func() {
		ms.statusLock.Lock()
		if err != nil {
			ms.status = oldStatus
		} else {
			ms.status = SCHED_STATUS_STOPPED
		}
		ms.statusLock.Unlock()
	}()

	if err != nil {
		return
	}
	ms.cancelFunc()
	ms.reqBufferPool.Close()
	ms.respBufferPool.Close()
	ms.itemBufferPool.Close()
	ms.errorBufferPool.Close()
	logger.Info("Scheduler has been stopped.")
	return nil
}

// checkAndSetStatus 用于状态的检查，并在条件满足时设置状态。
func (ms *myScheduler) checkAndSetStatus(wantedStatus Status) (oldStatus Status, err error) {
	ms.statusLock.Lock()
	defer ms.statusLock.Unlock()
	oldStatus = ms.status
	err = checkStatus(oldStatus, wantedStatus, nil)
	if err == nil {
		ms.status = wantedStatus
	}
	return
}

func (ms *myScheduler) Status() Status {
	ms.statusLock.RLock()
	status := ms.status
	ms.statusLock.RUnlock()
	return status
}

func (ms *myScheduler) ErrorChan() <-chan error {
	errBuffer := ms.errorBufferPool
	errCh := make(chan error, errBuffer.BufferCap())
	go func(errBuffer buffer.Pool, errCh chan error) {
		for {
			if ms.canceled() {
				close(errCh)
				break
			}
			datum, err := errBuffer.Get()
			if nil != err {
				logger.Warnln("The error buffer pool was closed. Break error reception.")
				close(errCh)
				break
			}
			err, ok := datum.(error)
			if !ok {
				errMsg := fmt.Sprintf("incorrect error type: %T", datum)
				sendError(errors.New(errMsg), "", ms.errorBufferPool)
				continue
			}
			if ms.canceled() {
				close(errCh)
				break
			}
			errCh <- err
		}
	}(errBuffer, errCh)
	return errCh
}

// 检测是否空闲
func (ms *myScheduler) Idle() bool {
	moduleMap := ms.registrar.GetAll()
	for _, mod := range moduleMap {
		if mod.HandlingNumber() > 0 {
			return false
		}
	}
	if ms.reqBufferPool.Total() > 0 ||
		ms.respBufferPool.Total() > 0 ||
		ms.itemBufferPool.Total() > 0 {
		return false
	}
	return true
}

func (ms *myScheduler) Summary() SchedSummary {
	return ms.summary
}

func (ms *myScheduler) registerModules(args ModuleArgs) error {
	for _, d := range args.Downloader {
		if nil == d {
			continue
		}
		ok, err := ms.registrar.Register(d)
		if nil != err {
			return genErrorByError(err)
		}
		if !ok {
			errMsg := fmt.Sprintf("Couldn't register downloader instance with MID %q!", d.ID())
			return genError(errMsg)
		}
	}
	logger.Infof("All downloads have been registered. (number: %d)", len(args.Downloader))
	for _, a := range args.Analyzers {
		if a == nil {
			continue
		}
		ok, err := ms.registrar.Register(a)
		if err != nil {
			return genErrorByError(err)
		}
		if !ok {
			errMsg := fmt.Sprintf("Couldn't register analyzer instance with MID %q!", a.ID())
			return genError(errMsg)
		}
	}
	logger.Infof("All analyzers have been registered. (number: %d)", len(args.Analyzers))
	for _, p := range args.Pipelines {
		if p == nil {
			continue
		}
		ok, err := ms.registrar.Register(p)
		if err != nil {
			return genErrorByError(err)
		}
		if !ok {
			errMsg := fmt.Sprintf("Couldn't register pipeline instance with MID %q!", p.ID())
			return genError(errMsg)
		}
	}
	logger.Infof("All pipelines have been registered. (number: %d)", len(args.Pipelines))
	return nil
}

func (ms *myScheduler) download() {

	go func() {
		for {
			if ms.canceled() {
				break
			}
			datum, err := ms.reqBufferPool.Get()
			if err != nil {
				logger.Warnln("The request buffer pool was closed. Break request reception.")
				break
			}
			req, ok := datum.(*module.Request)
			if !ok {
				errMsg := fmt.Sprintf("incorrect request type: %T", datum)
				sendError(errors.New(errMsg), "", ms.errorBufferPool)
			}
			ms.downloadOne(req)
		}
	}()
}

// downloadOne 会根据给定的请求执行下载并把响应放入响应缓冲池。
func (ms *myScheduler) downloadOne(req *module.Request) {
	if nil == req {
		return
	}
	if ms.canceled() {
		return
	}
	m, err := ms.registrar.Get(module.TYPE_DOWNLOADER)
	if err != nil || nil == m {
		errMsg := fmt.Sprintf("couldn't get a downloader: %s", err)
		sendError(errors.New(errMsg), "", ms.errorBufferPool)
		ms.sendReq(req)
		return
	}
	downloader, ok := m.(module.Downloader)
	if !ok {
		errMsg := fmt.Sprintf("incorrect downloader type: %T (MID: %s)", m, m.ID())
		sendError(errors.New(errMsg), m.ID(), ms.errorBufferPool)
		ms.sendReq(req)
		return
	}
	resp, err := downloader.Download(req)
	if nil != resp {
		sendResp(resp, ms.respBufferPool)
	}
	if err != nil {
		sendError(err, m.ID(), ms.errorBufferPool)
	}
}
func (ms *myScheduler) analyze() {
	go func() {
		for {
			if ms.canceled() {
				break
			}
			datum, err := ms.respBufferPool.Get()
			if nil != err {
				logger.Warnln("The response buffer pool was closed. Break response reception.")
				break
			}
			resp, ok := datum.(*module.Response)
			if !ok {
				errMsg := fmt.Sprintf("incorrect response type: %T", datum)
				sendError(errors.New(errMsg), "", ms.errorBufferPool)
			}
			ms.analyzeOne(resp)
		}
	}()
}

func (ms *myScheduler) analyzeOne(resp *module.Response) {
	if nil == resp {
		return
	}
	if ms.canceled() {
		return
	}
	m, err := ms.registrar.Get(module.TYPE_ANALYZER)
	if err != nil || m == nil {
		errMsg := fmt.Sprintf("couldn't get an analyzer: %s", err)
		sendError(errors.New(errMsg), "", ms.errorBufferPool)
		sendResp(resp, ms.respBufferPool)
		return
	}
	analyzer, ok := m.(module.Analyzer)
	if !ok {
		errMsg := fmt.Sprintf("incorrect analyzer type: %T (MID: %s)",
			m, m.ID())
		sendError(errors.New(errMsg), m.ID(), ms.errorBufferPool)
		sendResp(resp, ms.respBufferPool)
		return
	}
	data, errs := analyzer.Analyze(resp)
	if nil != data {
		for _, datum := range data {
			if nil == datum {
				continue
			}
			switch d := datum.(type) {
			case *module.Request:
				ms.sendReq(d)
			case module.Item:
				sendItem(d, ms.itemBufferPool)
			default:
				errMsg := fmt.Sprintf("Unsupported data type %T! (data: %#v)", d, d)
				sendError(errors.New(errMsg), m.ID(), ms.errorBufferPool)
			}
		}
	}
	if errs != nil {
		for _, err := range errs {
			sendError(err, m.ID(), ms.errorBufferPool)
		}
	}
}

func (ms *myScheduler) pick() {
	go func() {
		for {
			if ms.canceled() {
				break
			}
			datum, err := ms.itemBufferPool.Get()
			if err != nil {
				logger.Warnln("The item buffer pool was closed. Break item reception.")
				break
			}
			item, ok := datum.(module.Item)
			if !ok {
				errMsg := fmt.Sprintf("incorrect item type: %T", datum)
				sendError(errors.New(errMsg), "", ms.errorBufferPool)
			}
			ms.pickOne(item)
		}
	}()
}

func (ms *myScheduler) pickOne(item module.Item) {
	if ms.canceled() {
		return
	}
	m, err := ms.registrar.Get(module.TYPE_PIPELINE)
	if err != nil || m == nil {
		errMsg := fmt.Sprintf("couldn't get a pipeline: %s", err)
		sendError(errors.New(errMsg), "", ms.errorBufferPool)
		sendItem(item, ms.itemBufferPool)
		return
	}
	pipeline, ok := m.(module.Pipeline)
	if !ok {
		errMsg := fmt.Sprintf("incorrect pipeline type: %T (MID: %s)", m, m.ID())
		sendError(errors.New(errMsg), m.ID(), ms.errorBufferPool)
		sendItem(item, ms.itemBufferPool)
		return
	}
	errs := pipeline.Send(item)
	if errs != nil {
		for _, err := range errs {
			sendError(err, m.ID(), ms.errorBufferPool)
		}
	}
}

func sendItem(item module.Item, itemBufferPool buffer.Pool) bool {
	if item == nil || itemBufferPool == nil || itemBufferPool.Closed() {
		return false
	}
	go func(item module.Item) {
		if err := itemBufferPool.Put(item); err != nil {
			logger.Warnln("The item buffer pool was closed. Ignore item sending.")
		}
	}(item)
	return true
}

func sendResp(resp *module.Response, pool buffer.Pool) bool {
	if resp == nil || pool == nil || pool.Closed() {
		return false
	}
	go func(resp *module.Response) {
		if err := pool.Put(resp); err != nil {
			logger.Warnln("The response buffer pool was closed. Ignore response sending.")
		}
	}(resp)
	return true
}

// initBufferPool 用于按照给定的参数初始化缓冲池。
// 如果某个缓冲池可用且未关闭，就先关闭该缓冲池。
func (ms *myScheduler) initBufferPool(args DataArgs) {
	// 初始化请求缓冲池。
	if ms.reqBufferPool != nil && !ms.reqBufferPool.Closed() {
		ms.reqBufferPool.Close()
	}
	ms.reqBufferPool, _ = buffer.NewPool(args.ReqBufferCap, args.ReqMaxBufferNumber)
	logger.Infof("-- Request buffer pool: bufferCap: %d, maxBufferNumber: %d",
		ms.reqBufferPool.BufferCap(), ms.reqBufferPool.MaxBufferNumber())

	// 初始化响应缓冲池。
	if ms.respBufferPool != nil && !ms.respBufferPool.Closed() {
		ms.respBufferPool.Close()
	}
	ms.respBufferPool, _ = buffer.NewPool(args.ReqBufferCap, args.ReqMaxBufferNumber)
	logger.Infof("-- Request buffer pool: bufferCap: %d, maxBufferNumber: %d",
		ms.respBufferPool.BufferCap(), ms.respBufferPool.MaxBufferNumber())

	// 初始化条目缓冲池。
	if ms.itemBufferPool != nil && !ms.itemBufferPool.Closed() {
		ms.itemBufferPool.Close()
	}
	ms.itemBufferPool, _ = buffer.NewPool(
		args.ItemBufferCap, args.ItemMaxBufferNumber)
	logger.Infof("-- Item buffer pool: bufferCap: %d, maxBufferNumber: %d",
		ms.itemBufferPool.BufferCap(), ms.itemBufferPool.MaxBufferNumber())
	// 初始化错误缓冲池。
	if ms.errorBufferPool != nil && !ms.errorBufferPool.Closed() {
		ms.errorBufferPool.Close()
	}
	ms.errorBufferPool, _ = buffer.NewPool(
		args.ErrorBufferCap, args.ErrorMaxBufferNumber)
	logger.Infof("-- Error buffer pool: bufferCap: %d, maxBufferNumber: %d",
		ms.errorBufferPool.BufferCap(), ms.errorBufferPool.MaxBufferNumber())
}

func (ms *myScheduler) resetContext() {
	ms.ctx, ms.cancelFunc = context.WithCancel(context.Background())
}

// checkBufferPoolForStart 会检查缓冲池是否已为调度器的启动准备就绪。
// 如果某个缓冲池不可用，就直接返回错误值报告此情况。
// 如果某个缓冲池已关闭，就按照原先的参数重新初始化它。
func (ms *myScheduler) checkBufferPoolForStart() error {
	// 检查请求缓冲池。
	if ms.reqBufferPool == nil {
		return genError("nil request buffer pool")
	}
	if ms.reqBufferPool != nil && ms.reqBufferPool.Closed() {
		ms.reqBufferPool, _ = buffer.NewPool(
			ms.reqBufferPool.BufferCap(), ms.reqBufferPool.MaxBufferNumber())
	}
	// 检查响应缓冲池。
	if ms.respBufferPool == nil {
		return genError("nil response buffer pool")
	}
	if ms.respBufferPool != nil && ms.respBufferPool.Closed() {
		ms.respBufferPool, _ = buffer.NewPool(
			ms.respBufferPool.BufferCap(), ms.respBufferPool.MaxBufferNumber())
	}
	// 检查条目缓冲池。
	if ms.itemBufferPool == nil {
		return genError("nil item buffer pool")
	}
	if ms.itemBufferPool != nil && ms.itemBufferPool.Closed() {
		ms.itemBufferPool, _ = buffer.NewPool(
			ms.itemBufferPool.BufferCap(), ms.itemBufferPool.MaxBufferNumber())
	}
	// 检查错误缓冲池。
	if ms.errorBufferPool == nil {
		return genError("nil error buffer pool")
	}
	if ms.errorBufferPool != nil && ms.errorBufferPool.Closed() {
		ms.errorBufferPool, _ = buffer.NewPool(
			ms.errorBufferPool.BufferCap(), ms.errorBufferPool.MaxBufferNumber())
	}
	return nil
}

// canceled 用于判断调度器的上下文是否已被取消。
func (ms *myScheduler) canceled() bool {
	select {
	case <-ms.ctx.Done():
		return true
	default:
		return false
	}
}

func (ms *myScheduler) sendReq(req *module.Request) bool {
	if req == nil {
		return false
	}
	if ms.canceled() {
		return false
	}
	httpReq := req.HTTPReq()
	if httpReq == nil {
		logger.Warnln("Ignore the request! Its HTTP request is invalid!")
		return false
	}
	reqURL := httpReq.URL
	if reqURL == nil {
		logger.Warnln("Ignore the request! Its URL is invalid!")
		return false
	}
	scheme := strings.ToLower(reqURL.Scheme)
	if scheme != "http" && scheme != "https" {
		logger.Warnf("Ignore the request! Its URL scheme is %q, but should be %q or %q. (URL: %s)\n",
			scheme, "http", "https", reqURL)
		return false
	}
	// 防止重复爬取
	u := reqURL.String()
	v := ms.urlMap.Get(u)
	if v != nil {
		logger.Warnf("Ignore the request! Its URL is repeated. (URL: %s)\n", reqURL)
		return false
	}
	pd, _ := getPrimaryDomain(httpReq.Host)
	// 防止无限扩张
	if ms.acceptedDomainMap.Get(pd) == nil {
		if pd == "bing.net" {
			panic(httpReq.URL)
		}
		logger.Warnf("Ignore the request! Its host %q is not in accepted primary domain map. (URL: %s)\n",
			httpReq.Host, reqURL)
		return false
	}
	// 深度限制
	if req.Depth() > ms.maxDepth {
		logger.Warnf("Ignore the request! Its depth %d is greater than %d. (URL: %s)\n",
			req.Depth(), ms.maxDepth, reqURL)
		return false
	}
	// 开始处理
	go func(req *module.Request) {
		if err := ms.reqBufferPool.Put(req); nil != err {
			logger.Warnln("The request buffer pool was closed. Ignore request sending.")
		}
	}(req)
	_, _ = ms.urlMap.Put(reqURL.String(), struct{}{})
	return true
}
