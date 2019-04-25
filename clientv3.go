package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
)

type clientv3 struct {
	client  *etcdv3.Client
	ctx     context.Context
	timeout time.Duration
}

// NewClient returns Client with a connection to the named machines. It will
// return an error if a connection to the cluster cannot be made. The parameter
// machines needs to be a full URL with schemas. e.g. "http://localhost:2379"
// will work, but "localhost:2379" will not.
func NewClientV3(ctx context.Context, machines []string, options ClientOptions) (Client, error) {
	if options.DialTimeout == 0 {
		options.DialTimeout = defaultTTL
	}
	if options.DialKeepAlive == 0 {
		options.DialKeepAlive = defaultTTL
	}
	if options.HeaderTimeoutPerRequest == 0 {
		options.HeaderTimeoutPerRequest = defaultTTL
	}

	var tlsCfg *tls.Config
	if options.Cert != "" && options.Key != "" {
		tlsCert, err := tls.LoadX509KeyPair(options.Cert, options.Key)
		if err != nil {
			return nil, err
		}
		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
		}
		if caCertCt, err := ioutil.ReadFile(options.CACert); err == nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCertCt)
			tlsCfg.RootCAs = caCertPool
		}
	}

	ce, err := etcdv3.New(etcdv3.Config{
		Endpoints:            machines,
		DialTimeout:          options.DialTimeout,
		DialKeepAliveTime:    options.DialKeepAlive,
		DialKeepAliveTimeout: options.HeaderTimeoutPerRequest,
		TLS:                  tlsCfg,
	})
	if err != nil {
		return nil, err
	}

	return &clientv3{
		client:  ce,
		ctx:     ctx,
		timeout: options.HeaderTimeoutPerRequest,
	}, nil
}

// GetEntries implements the etcd Client interface.
func (c *clientv3) GetEntries(key string) ([]string, error) {

	if c.client == nil {
		return nil, ErrNilClient
	}

	// set the timeout for this requisition
	timeoutCtx, cancel := context.WithTimeout(c.ctx, c.timeout)
	resp, err := c.client.Get(timeoutCtx, key, etcdv3.WithPrefix())
	cancel()

	if err != nil {
		return nil, err
	}

	// Special case. Note that it's possible that len(resp.Node.Nodes) == 0 and
	// resp.Node.Value is also empty, in which case the key is empty and we
	// should not return any entries.
	if len(resp.Kvs) == 0 || resp.Count != int64(len(resp.Kvs)) {
		return nil, nil
	}

	entries := make([]string, resp.Count)
	for i, ev := range resp.Kvs {
		entries[i] = string(ev.Value[:])
	}
	return entries, nil
}

// WatchPrefix implements the etcd Client interface.
func (c *clientv3) WatchPrefix(prefix string, ch chan struct{}) {

	if c.client == nil {
		return
	}
	watch := c.client.Watch(c.ctx, prefix, etcdv3.WithPrefix())
	ch <- struct{}{} // make sure caller invokes GetEntries
	for _ = range watch {
		ch <- struct{}{}
	}
}
