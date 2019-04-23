package etcd

import (
	"context"
	"testing"
	"time"
)

func TestNewClient_withDefaultsV3(t *testing.T) {
	client, err := NewClientV3(
		context.Background(),
		[]string{"http://irrelevant:12345"},
		ClientOptions{},
	)
	if err == nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}
	if client != nil {
		t.Fatal("expected new Client, got nil")
	}
}

// NewClient should fail when providing invalid or missing endpoints.
func TestOptionsV3(t *testing.T) {
	a, err := NewClientV3(
		context.Background(),
		[]string{},
		ClientOptions{
			Cert:                    "",
			Key:                     "",
			CACert:                  "",
			DialTimeout:             2 * time.Second,
			DialKeepAlive:           2 * time.Second,
			HeaderTimeoutPerRequest: 2 * time.Second,
		},
	)
	if err == nil {
		t.Errorf("expected error: %v", err)
	}
	if a != nil {
		t.Fatalf("expected client to be nil on failure")
	}

	_, err = NewClientV3(
		context.Background(),
		[]string{"http://irrelevant:12345"},
		ClientOptions{
			Cert:                    "blank.crt",
			Key:                     "blank.key",
			CACert:                  "blank.CACert",
			DialTimeout:             2 * time.Second,
			DialKeepAlive:           2 * time.Second,
			HeaderTimeoutPerRequest: 2 * time.Second,
		},
	)
	if err == nil {
		t.Errorf("expected error: %v", err)
	}
}

// ---------------------------------------------------------------------------------------------------------------------

func newFakeClientV3(ctx context.Context) Client {
	return &clientv3{
		client:  nil,
		ctx:     ctx,
		timeout: 3 * time.Second,
	}
}

func TestWatchPrefixV3(t *testing.T) {

	cv3 := newFakeClientV3(context.Background())

	ch := make(chan struct{})
	cv3.WatchPrefix("prefix", ch)
}

func TestGetEntriesV3(t *testing.T) {
	cv3 := newFakeClientV3(context.Background())

	res, err := cv3.GetEntries("prefix")
	if res != nil || err == nil {
		t.Errorf("expected client error")
	}
}
