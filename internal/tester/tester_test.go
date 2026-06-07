package tester

import (
	"encoding/json"
	"testing"
)

func TestBuildConfigDisablesMuxByDefault(t *testing.T) {
	configJSON, err := buildConfig(json.RawMessage(`{
		"protocol": "freedom",
		"tag": "direct",
		"settings": {},
		"mux": {
			"enabled": true,
			"concurrency": 8,
			"xudpConcurrency": 4
		}
	}`), false)
	if err != nil {
		t.Fatalf("buildConfig failed: %v", err)
	}

	outbound := firstOutbound(t, configJSON)
	mux := outbound["mux"].(map[string]any)
	if mux["enabled"].(bool) {
		t.Fatalf("mux.enabled = true, want false")
	}
	if got := int(mux["concurrency"].(float64)); got != -1 {
		t.Fatalf("mux.concurrency = %d, want -1", got)
	}
	if got := int(mux["xudpConcurrency"].(float64)); got != -1 {
		t.Fatalf("mux.xudpConcurrency = %d, want -1", got)
	}
	if got := mux["xudpProxyUDP443"].(string); got != "skip" {
		t.Fatalf("mux.xudpProxyUDP443 = %q, want skip", got)
	}
}

func TestBuildConfigPreservesMuxWhenAllowed(t *testing.T) {
	configJSON, err := buildConfig(json.RawMessage(`{
		"protocol": "freedom",
		"tag": "direct",
		"settings": {},
		"mux": {
			"enabled": true,
			"concurrency": 8
		}
	}`), true)
	if err != nil {
		t.Fatalf("buildConfig failed: %v", err)
	}

	outbound := firstOutbound(t, configJSON)
	mux := outbound["mux"].(map[string]any)
	if !mux["enabled"].(bool) {
		t.Fatalf("mux.enabled = false, want true")
	}
	if got := int(mux["concurrency"].(float64)); got != 8 {
		t.Fatalf("mux.concurrency = %d, want 8", got)
	}
}

func TestBuildConfigReturnsErrorOnXrayPanic(t *testing.T) {
	_, err := buildConfig(json.RawMessage(`{
		"tag": "bad-vless",
		"protocol": "vless",
		"settings": {
			"vnext": [{
				"address": "86.104.74.157",
				"port": 509,
				"users": [{
					"id": "3aaf0a8b-9a70-4cf0-9f3f-8eb7ccd3fb63",
					"email": "t@t.tt",
					"security": "auto",
					"encryption": "mlkem768x25519plus.native.0rtt..."
				}]
			}]
		},
		"streamSettings": {
			"network": "ws",
			"wsSettings": {
				"path": "/api",
				"headers": {}
			}
		}
	}`), false)
	if err == nil {
		t.Fatal("buildConfig succeeded, want error")
	}
}

func firstOutbound(t *testing.T, configJSON []byte) map[string]any {
	t.Helper()

	var config struct {
		Outbounds []map[string]any `json:"outbounds"`
	}
	if err := json.Unmarshal(configJSON, &config); err != nil {
		t.Fatalf("decode generated config: %v", err)
	}
	if len(config.Outbounds) != 1 {
		t.Fatalf("outbounds length = %d, want 1", len(config.Outbounds))
	}
	return config.Outbounds[0]
}
