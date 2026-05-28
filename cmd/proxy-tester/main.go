package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

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
	var geoIP2Path string
	var downloadTimeoutMS int
	var connectTimeoutMS int

	flag.StringVar(&cfg.TestType, "test-type", "url", "test type: url or speed")
	flag.StringVar(&cfg.TestURL, "url", "", "URL to test")
	flag.IntVar(&cfg.Retries, "retries", 1, "number of test attempts per outbound")
	flag.Var(&exitIPURLs, "exit-ip-url", "URL used to detect exit IP; may be repeated or comma-separated")
	flag.StringVar(&outboundsFile, "outbounds-file", "", "JSON file with outbounds; stdin is used when omitted or set to '-'")
	flag.StringVar(&geoIP2Path, "geoip2-db-path", "", "optional path to GeoIP2/GeoLite2 City database (.mmdb)")
	flag.IntVar(&downloadTimeoutMS, "download-timeout", 30000, "timeout for speed-test download")
	flag.IntVar(&connectTimeoutMS, "connect-timeout", 10000, "timeout for URL test and exit-IP requests")
	flag.IntVar(&cfg.Parallelism, "parallelism", 1, "maximum number of outbounds tested concurrently")
	flag.Float64Var(&cfg.MinSpeedMbps, "min-speed-mbps", 0, "optional minimum speed threshold; 0 disables speed_below_threshold")
	flag.IntVar(&cfg.MaxLatencyMS, "max-latency", 0, "optional maximum latency threshold; 0 disables latency_exceeded")
	flag.BoolVar(&cfg.AllowMux, "allow-mux", false, "preserve outbound mux settings instead of disabling mux during tests")
	flag.Parse()

	cfg.ExitIPURLs = exitIPURLs
	cfg.DownloadTimeout = downloadTimeoutMS
	cfg.ConnectTimeout = connectTimeoutMS
	cfg.GeoIP2DBPath = geoIP2Path

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
	enc.SetIndent("", "")
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
