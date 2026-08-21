package integration

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	pusher "github.com/pusher/pusher-http-go/v5"
)

type provisionedApplication struct {
	AppID     string `json:"appId"`
	AppKey    string `json:"appKey"`
	AppSecret string `json:"appSecret"`
}

func randomID(t *testing.T, prefix string) string {
	t.Helper()
	value := make([]byte, 8)
	if _, err := rand.Read(value); err != nil {
		t.Fatalf("generate random ID: %v", err)
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(value))
}

func controlRequest(
	t *testing.T,
	client *http.Client,
	method string,
	url string,
	token string,
	body io.Reader,
) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("create control request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("perform control request: %v", err)
	}
	return response
}

func TestOfficialGoSDK(t *testing.T) {
	if os.Getenv("PUSHER_GO_E2E") != "1" {
		t.Skip("set PUSHER_GO_E2E=1 to run against a live durable-pusher server")
	}

	host := os.Getenv("PUSHER_GO_E2E_HOST")
	if host == "" {
		host = "127.0.0.1:1337"
	}
	scheme := "http"
	secure := os.Getenv("PUSHER_GO_E2E_TLS") == "1"
	if secure {
		scheme = "https"
	}
	controlToken := os.Getenv("PUSHER_CONTROL_TOKEN")
	if controlToken == "" {
		controlToken = "control-token"
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}
	appID := randomID(t, "go")
	controlURL := fmt.Sprintf("%s://%s/control/v1/apps", scheme, host)
	createBody, err := json.Marshal(map[string]any{
		"appId":        appID,
		"jurisdiction": "us",
		"locationHint": "wnam",
		"name":         "Official Go SDK integration",
	})
	if err != nil {
		t.Fatalf("encode application request: %v", err)
	}
	createResponse := controlRequest(
		t,
		httpClient,
		http.MethodPost,
		controlURL,
		controlToken,
		bytes.NewReader(createBody),
	)
	defer createResponse.Body.Close()
	if createResponse.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResponse.Body)
		t.Fatalf("create application: status %d: %s", createResponse.StatusCode, body)
	}
	var application provisionedApplication
	if err := json.NewDecoder(createResponse.Body).Decode(&application); err != nil {
		t.Fatalf("decode application: %v", err)
	}
	t.Cleanup(func() {
		response := controlRequest(
			t,
			httpClient,
			http.MethodDelete,
			controlURL+"/"+appID,
			controlToken,
			nil,
		)
		defer response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Errorf("delete application: status %d", response.StatusCode)
		}
	})

	client := pusher.Client{
		AppID:      application.AppID,
		Key:        application.AppKey,
		Secret:     application.AppSecret,
		Host:       host,
		Secure:     secure,
		HTTPClient: httpClient,
	}
	channel := randomID(t, "go-channel")
	if err := client.Trigger(channel, "go-event", map[string]string{"message": "from-go"}); err != nil {
		t.Fatalf("trigger event with official Go SDK: %v", err)
	}

	info := "subscription_count"
	state, err := client.Channel(channel, pusher.ChannelParams{Info: &info})
	if err != nil {
		t.Fatalf("query channel with official Go SDK: %v", err)
	}
	if state.Occupied || state.SubscriptionCount != 0 {
		t.Fatalf("unexpected channel state: %+v", state)
	}

	_, err = client.TriggerBatch([]pusher.Event{
		{Channel: channel, Name: "go-batch-one", Data: map[string]int{"sequence": 1}},
		{Channel: channel, Name: "go-batch-two", Data: map[string]int{"sequence": 2}},
	})
	if err != nil {
		t.Fatalf("trigger batch with official Go SDK: %v", err)
	}
}
