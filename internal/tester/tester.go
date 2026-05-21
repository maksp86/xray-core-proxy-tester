package tester

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/geoip2-golang"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	_ "github.com/xtls/xray-core/main/distro/all"
)

const (
	ReasonOK                  = "ok"
	ReasonInvalidOutbound     = "invalid_outbound"
	ReasonTestFailed          = "test_failed"
	ReasonSpeedBelowThreshold = "speed_below_threshold"
	ReasonLatencyExceeded     = "latency_exceeded"
)

type Config struct {
	TestType        string
	TestURL         string
	Retries         int
	ExitIPURLs      []string
	DownloadTimeout float64
	ConnectTimeout  float64
	Parallelism     int
	MinSpeedMbps    float64
	MaxLatencyMS    float64
	GeoIP2DBPath    string
}

type Outbound struct {
	Tag string
	Raw json.RawMessage
}

type Result struct {
	Result  bool     `json:"result"`
	Speed   *float64 `json:"speed"`
	Latency *float64 `json:"latency"`
	ExitIP  *string  `json:"exit-ip"`
	Country *string  `json:"country"`
	City    *string  `json:"city"`
	Reason  string   `json:"reason"`
}

func ParseOutbounds(data []byte) ([]Outbound, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, errors.New("outbounds input is empty")
	}

	var rawOutbounds []json.RawMessage
	if data[0] == '[' {
		if err := json.Unmarshal(data, &rawOutbounds); err != nil {
			return nil, fmt.Errorf("decode outbounds array: %w", err)
		}
	} else {
		var wrapper struct {
			Outbounds []json.RawMessage `json:"outbounds"`
		}
		if err := json.Unmarshal(data, &wrapper); err != nil {
			return nil, fmt.Errorf("decode outbounds object: %w", err)
		}
		rawOutbounds = wrapper.Outbounds
	}
	if len(rawOutbounds) == 0 {
		return nil, errors.New("no outbounds found")
	}

	outbounds := make([]Outbound, 0, len(rawOutbounds))
	seen := make(map[string]int, len(rawOutbounds))
	for i, raw := range rawOutbounds {
		tag := extractTag(raw)
		if tag == "" {
			tag = fmt.Sprintf("outbound-%d", i+1)
			raw = ensureTag(raw, tag)
		}
		if n := seen[tag]; n > 0 {
			seen[tag] = n + 1
			tag = fmt.Sprintf("%s-%d", tag, n+1)
			raw = ensureTag(raw, tag)
		} else {
			seen[tag] = 1
		}
		outbounds = append(outbounds, Outbound{Tag: tag, Raw: raw})
	}
	return outbounds, nil
}

func Run(ctx context.Context, cfg Config, outbounds []Outbound) (map[string]Result, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	geoResolver, err := newGeoResolver(cfg.GeoIP2DBPath)
	if err != nil {
		return nil, err
	}
	defer geoResolver.Close()

	results := make(map[string]Result, len(outbounds))
	jobs := make(chan Outbound)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < cfg.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for outbound := range jobs {
				result := testOutbound(ctx, cfg, outbound, geoResolver)
				mu.Lock()
				results[outbound.Tag] = result
				mu.Unlock()
			}
		}()
	}
	for _, outbound := range outbounds {
		jobs <- outbound
	}
	close(jobs)
	wg.Wait()
	return results, nil
}

func (cfg *Config) validate() error {
	cfg.TestType = strings.ToLower(strings.TrimSpace(cfg.TestType))
	if cfg.TestType != "url" && cfg.TestType != "speed" {
		return errors.New("test-type must be url or speed")
	}
	if strings.TrimSpace(cfg.TestURL) == "" {
		return errors.New("url is required")
	}
	if _, err := url.ParseRequestURI(cfg.TestURL); err != nil {
		return fmt.Errorf("invalid test url: %w", err)
	}
	if cfg.Retries < 1 {
		cfg.Retries = 1
	}
	if cfg.Parallelism < 1 {
		cfg.Parallelism = 1
	}
	if cfg.DownloadTimeout <= 0 {
		cfg.DownloadTimeout = 30000
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10000
	}
	return nil
}

func testOutbound(ctx context.Context, cfg Config, outbound Outbound, geoResolver *geoResolver) Result {
	configJSON, err := buildConfig(outbound.Raw)
	if err != nil {
		return Result{Result: false, Reason: ReasonInvalidOutbound}
	}

	instance, err := core.StartInstance("json", configJSON)
	if err != nil {
		return Result{Result: false, Reason: ReasonInvalidOutbound}
	}
	defer instance.Close()

	transport := xrayHTTPTransport(instance, cfg.ConnectTimeout)
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	if cfg.TestType == "speed" {
		return runSpeedTest(ctx, client, cfg)
	}
	return runURLTest(ctx, client, cfg, geoResolver)
}

func runURLTest(ctx context.Context, client *http.Client, cfg Config, geoResolver *geoResolver) Result {
	var lastErr error
	for attempt := 0; attempt < cfg.Retries; attempt++ {
		timeout := time.Duration(cfg.ConnectTimeout) * time.Millisecond
		latency, err := requestLatency(ctx, client, cfg.TestURL, timeout, false)
		if err == nil {
			result := Result{Result: true, Latency: &latency, Reason: ReasonOK}
			if cfg.MaxLatencyMS > 0 && latency > cfg.MaxLatencyMS {
				result.Result = false
				result.Reason = ReasonLatencyExceeded
			}
			result.ExitIP = detectExitIP(ctx, client, cfg.ExitIPURLs, timeout)
			result.Country, result.City = geoResolver.Lookup(result.ExitIP)
			return result
		}
		lastErr = err
	}
	_ = lastErr
	return Result{Result: false, Reason: ReasonTestFailed}
}

type geoResolver struct {
	db *geoip2.Reader
}

func newGeoResolver(dbPath string) (*geoResolver, error) {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return &geoResolver{}, nil
	}
	db, err := geoip2.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open geoip2 db %q: %w", dbPath, err)
	}
	return &geoResolver{db: db}, nil
}

func (r *geoResolver) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

func (r *geoResolver) Lookup(ip *string) (*string, *string) {
	if r == nil || r.db == nil || ip == nil {
		return nil, nil
	}
	parsedIP := net.ParseIP(strings.TrimSpace(*ip))
	if parsedIP == nil {
		return nil, nil
	}
	record, err := r.db.City(parsedIP)
	if err != nil {
		return nil, nil
	}
	var country *string
	if record.Country.IsoCode != "" {
		value := record.Country.IsoCode
		country = &value
	}
	var city *string
	if name, ok := record.City.Names["en"]; ok && strings.TrimSpace(name) != "" {
		value := name
		city = &value
	}
	return country, city
}

func runSpeedTest(ctx context.Context, client *http.Client, cfg Config) Result {
	var lastErr error
	for attempt := 0; attempt < cfg.Retries; attempt++ {
		speed, err := downloadSpeed(ctx, client, cfg.TestURL, time.Duration(cfg.DownloadTimeout)*time.Millisecond)
		if err == nil {
			result := Result{Result: true, Speed: &speed, Reason: ReasonOK}
			if cfg.MinSpeedMbps > 0 && speed < cfg.MinSpeedMbps {
				result.Result = false
				result.Reason = ReasonSpeedBelowThreshold
			}
			return result
		}
		lastErr = err
	}
	_ = lastErr
	return Result{Result: false, Reason: ReasonTestFailed}
}

func requestLatency(ctx context.Context, client *http.Client, target string, timeout time.Duration, readBody bool) (float64, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, target, nil)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		closeResponseBody(resp)
		return 0, err
	}
	defer closeResponseBody(resp)
	if readBody {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	}
	latency := float64(time.Since(start).Microseconds()) / 1000.0
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return latency, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return latency, nil
}

func downloadSpeed(ctx context.Context, client *http.Client, target string, timeout time.Duration) (float64, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, target, nil)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		closeResponseBody(resp)
		return 0, err
	}
	defer closeResponseBody(resp)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return 0, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	bytesRead, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		return 0, err
	}
	seconds := time.Since(start).Seconds()
	if seconds <= 0 {
		return 0, errors.New("download finished too quickly to measure speed")
	}
	return float64(bytesRead) * 8 / seconds / 1_000_000, nil
}

func detectExitIP(ctx context.Context, client *http.Client, urls []string, timeout time.Duration) *string {
	for _, target := range urls {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, target, nil)
		if err != nil {
			cancel()
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			closeResponseBody(resp)
			cancel()
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		closeResponseBody(resp)
		cancel()
		if readErr == nil && resp.StatusCode >= 200 && resp.StatusCode <= 299 {
			ip := strings.TrimSpace(string(body))
			if ip != "" {
				return &ip
			}
		}
	}
	return nil
}

func closeResponseBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func buildConfig(outbound json.RawMessage) ([]byte, error) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(outbound, &probe); err != nil {
		return nil, err
	}
	if _, ok := probe["protocol"]; !ok {
		return nil, errors.New("outbound protocol is required")
	}
	config := map[string]any{
		"log":       map[string]any{"loglevel": "none", "access": "none", "error": "none"},
		"outbounds": []json.RawMessage{outbound},
	}
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	if _, err := core.LoadConfig("json", bytes.NewReader(data)); err != nil {
		return nil, err
	}
	return data, nil
}

func xrayHTTPTransport(instance *core.Instance, timeout_ms float64) *http.Transport {
	timeout := time.Duration(timeout_ms) * time.Millisecond
	return &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			dest, err := xnet.ParseDestination(network + ":" + address)
			if err != nil {
				return nil, err
			}
			conn, err := core.Dial(ctx, instance, dest)
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
	}
}

func extractTag(raw json.RawMessage) string {
	var meta struct {
		Tag string `json:"tag"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return ""
	}
	return strings.TrimSpace(meta.Tag)
}

func ensureTag(raw json.RawMessage, tag string) json.RawMessage {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	obj["tag"] = tag
	updated, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return updated
}

func SortedKeys(results map[string]Result) []string {
	keys := make([]string, 0, len(results))
	for key := range results {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
