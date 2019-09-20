package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	lib "go-spider/examples/finder/internal"
	"go-spider/examples/finder/monitor"
	"go-spider/helper/log"
	schedule "go-spider/scheduler"
)

// 命令参数。
var (
	firstURL string
	domains  string
	depth    uint
	dirPath  string
)

// 日志记录器。
var logger = log.DLogger()

func init() {
	flag.StringVar(&firstURL, "first", "http://zhihu.sogou.com/zhihu?query=golang+logo",
		"The first URL which you want to access.")
	flag.StringVar(&domains, "domains", "zhihu.com",
		"The primary domains which you accepted. "+
			"Please using comma-separated multiple domains.")
	flag.UintVar(&depth, "depth", 3,
		"The depth for crawling.")
	flag.StringVar(&dirPath, "dir", "./pictures",
		"The path which you want to save the image files.")
}

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\tfinder [flags] \n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()
	// 创建调度器。
	scheduler := schedule.NewScheduler()
	// 准备调度器的初始化参数。
	domainParts := strings.Split(domains, ",")
	acceptedDomains := make([]string, 0)
	for _, domain := range domainParts {
		domain = strings.TrimSpace(domain)
		if domain != "" {
			acceptedDomains =
				append(acceptedDomains, domain)
		}
	}
	requestArgs := schedule.RequestArgs{
		AcceptedDomains: acceptedDomains,
		MaxDepth:        uint32(depth),
	}
	dataArgs := schedule.DataArgs{
		ReqBufferCap:         50,
		ReqMaxBufferNumber:   1000,
		RespBufferCap:        50,
		RespMaxBufferNumber:  10,
		ItemBufferCap:        50,
		ItemMaxBufferNumber:  100,
		ErrorBufferCap:       50,
		ErrorMaxBufferNumber: 1,
	}
	downloader, err := lib.GetDownloaders(1)
	if err != nil {
		logger.Fatalf("An error occurs when creating downloader: %s", err)
	}
	analyzers, err := lib.GetAnalyzers(1)
	if err != nil {
		logger.Fatalf("An error occurs when creating analyzers: %s", err)
	}
	pipelines, err := lib.GetPipelines(1, dirPath)
	if err != nil {
		logger.Fatalf("An error occurs when creating pipelines: %s", err)
	}
	moduleArgs := schedule.ModuleArgs{
		Downloader: downloader,
		Analyzers:  analyzers,
		Pipelines:  pipelines,
	}
	// 初始化调度器。
	err = scheduler.Init(
		requestArgs,
		dataArgs,
		moduleArgs)
	if err != nil {
		logger.Fatalf("An error occurs when initializing scheduler: %s", err)
	}
	// 准备监控参数。
	checkInterval := time.Second
	summarizeInterval := 100 * time.Millisecond
	maxIdleCount := uint(5)
	// 开始监控。
	checkCountChan := monitor.Monitor(
		scheduler,
		checkInterval,
		summarizeInterval,
		maxIdleCount,
		true,
		lib.Record)
	// 准备调度器的启动参数。
	firstHTTPReq, err := http.NewRequest("GET", firstURL, nil)
	if err != nil {
		logger.Fatalln(err)
		return
	}
	// 开启调度器
	err = scheduler.Start(firstHTTPReq)
	if err != nil {
		logger.Fatalf("An error occurs when starting scheduler: %s", err)
	}
	// 等待监控结束。
	<-checkCountChan
}
