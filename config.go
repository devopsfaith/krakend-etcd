package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/devopsfaith/krakend/config"
)

// Code taken from https://github.com/go-kit/kit/blob/master/sd/etcd/client.go

const defaultTTL = 3 * time.Second

// Client is a wrapper around the etcd client.
type Client interface {
	// GetEntries queries the given prefix in etcd and returns a slice
	// containing the values of all keys found, recursively, underneath that
	// prefix.
	GetEntries(prefix string) ([]string, error)

	// WatchPrefix watches the given prefix in etcd for changes. When a change
	// is detected, it will signal on the passed channel. Clients are expected
	// to call GetEntries to update themselves with the latest set of complete
	// values. WatchPrefix will always send an initial sentinel value on the
	// channel after establishing the watch, to ensure that clients always
	// receive the latest set of values. WatchPrefix will block until the
	// context passed to the NewClient constructor is terminated.
	WatchPrefix(prefix string, ch chan struct{})
}

// ClientOptions defines options for the etcd client. All values are optional.
// If any duration is not specified, a default of 3 seconds will be used.
type ClientOptions struct {
	Cert                    string
	Key                     string
	CACert                  string
	DialTimeout             time.Duration
	DialKeepAlive           time.Duration
	DialKeepAliveTimeout    time.Duration
	HeaderTimeoutPerRequest time.Duration
}

// Namespace is the key to use to store and access the custom config data
const Namespace = "github_com/devopsfaith/krakend-etcd"

var (
	// ErrNoConfig is the error to be returned when there is no config with the etcd namespace
	ErrNoConfig = fmt.Errorf("unable to create the etcd client: no config")
	// ErrBadConfig is the error to be returned when the config is not well defined
	ErrBadConfig = fmt.Errorf("unable to create the etcd client with the received config")
	// ErrNoMachines is the error to be returned when the config has not defined one or more servers
	ErrNoMachines = fmt.Errorf("unable to create the etcd client without a set of servers")
	// ErrNilClient is the error to be nil client
	ErrNilClient = fmt.Errorf("nil etcd client")
)

// New creates an etcd client with the config extracted from the extra config param
func New(ctx context.Context, e config.ExtraConfig) (Client, error) {
	v, ok := e[Namespace]
	if !ok {
		return nil, ErrNoConfig
	}
	tmp, ok := v.(map[string]interface{})
	if !ok {
		return nil, ErrBadConfig
	}
	machines, err := parseMachines(tmp)
	if err != nil {
		return nil, err
	}
	version, err := parseVersion(tmp)
	if err != nil {
		return nil, err
	}

	if version == "v3" {
		return NewClientV3(ctx, machines, parseOptions(tmp))
	}
	return NewClient(ctx, machines, parseOptions(tmp))
}

func parseVersion(cfg map[string]interface{}) (string, error) {
	value, ok := cfg["client_version"]
	if !ok {
		return "v2", nil
	}
	result := value.(string)
	if result != "v2" && result != "v3" {
		result = "v2"
	}
	return result, nil
}

func parseMachines(cfg map[string]interface{}) ([]string, error) {
	result := []string{}
	machines, ok := cfg["machines"]
	if !ok {
		return result, ErrNoMachines
	}
	ms, ok := machines.([]interface{})
	if !ok {
		return result, ErrNoMachines
	}
	for _, m := range ms {
		if machine, ok := m.(string); ok {
			result = append(result, machine)
		}
	}
	if len(result) == 0 {
		return result, ErrNoMachines
	}
	return result, nil
}

func parseOptions(cfg map[string]interface{}) ClientOptions {
	options := ClientOptions{}
	v, ok := cfg["options"]
	if !ok {
		return options
	}
	tmp := v.(map[string]interface{})

	if o, ok := tmp["cert"]; ok {
		options.Cert = o.(string)
	}

	if o, ok := tmp["key"]; ok {
		options.Key = o.(string)
	}

	if o, ok := tmp["cacert"]; ok {
		options.CACert = o.(string)
	}

	if o, ok := tmp["dial_timeout"]; ok {
		if d, err := parseDuration(o); err == nil {
			options.DialTimeout = d
		}
	}

	if o, ok := tmp["dial_keepalive"]; ok {
		if d, err := parseDuration(o); err == nil {
			options.DialKeepAlive = d
		}
	}

	if o, ok := tmp["header_timeout"]; ok {
		if d, err := parseDuration(o); err == nil {
			options.HeaderTimeoutPerRequest = d
		}
	}
	return options
}

func parseDuration(v interface{}) (time.Duration, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("unable to parse %v as a time.Duration\n", v)
	}
	return time.ParseDuration(s)
}
