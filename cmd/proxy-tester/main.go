package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/example/xray-core-proxy-tester/internal/tester"
)

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(value string) error {
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			*s = append(*s, item)
		}
	}
	return nil
}

func main() {
	var exitIPURLs stringList
	cfg := tester.Config{}
	var outboundsFile string
	var downloadTimeout time.Duration
	var connectTimeout time.Duration

	flag.StringVar(&cfg.TestType, "test-type", "url", "test type: url or speed")
	flag.StringVar(&cfg.TestURL, "url", "", "URL to test")
	flag.IntVar(&cfg.Retries, "retries", 1, "number of test attempts per outbound")
	flag.Var(&exitIPURLs, "exit-ip-url", "URL used to detect exit IP; may be repeated or comma-separated")
	flag.StringVar(&outboundsFile, "outbounds-file", "", "JSON file with outbounds; stdin is used when omitted or set to '-'")
	flag.DurationVar(&downloadTimeout, "download-timeout", 30*time.Second, "timeout for speed-test download")
	flag.DurationVar(&connectTimeout, "connect-timeout", 10*time.Second, "timeout for URL test and exit-IP requests")
	flag.IntVar(&cfg.Parallelism, "parallelism", 1, "maximum number of outbounds tested concurrently")
	flag.Float64Var(&cfg.MinSpeedMbps, "min-speed-mbps", 0, "optional minimum speed threshold; 0 disables speed_below_threshold")
	flag.Float64Var(&cfg.MaxLatencyMS, "max-latency-ms", 0, "optional maximum latency threshold; 0 disables latency_exceeded")
	flag.Parse()

	cfg.ExitIPURLs = exitIPURLs
	cfg.DownloadTimeout = downloadTimeout
	cfg.ConnectTimeout = connectTimeout

	input, err := readInput(outboundsFile)
	if err != nil {
		fatal(err)
	}
	outbounds, err := tester.ParseOutbounds(input)
	if err != nil {
		fatal(err)
	}

	results, err := tester.Run(context.Background(), cfg, outbounds)
	if err != nil {
		fatal(err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		fatal(err)
	}
}

func readInput(path string) ([]byte, error) {
	if path == "" || path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
