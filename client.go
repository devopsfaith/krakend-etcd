package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"net/http"

	etcd "github.com/coreos/etcd/client"
)

type client struct {
	keysAPI etcd.KeysAPI
	ctx     context.Context
}

// NewClient returns Client with a connection to the named machines. It will
// return an error if a connection to the cluster cannot be made. The parameter
// machines needs to be a full URL with schemas. e.g. "http://localhost:2379"
// will work, but "localhost:2379" will not.
func NewClient(ctx context.Context, machines []string, options ClientOptions) (Client, error) {
	if options.DialTimeout == 0 {
		options.DialTimeout = defaultTTL
	}
	if options.DialKeepAlive == 0 {
		options.DialKeepAlive = defaultTTL
	}
	if options.HeaderTimeoutPerRequest == 0 {
		options.HeaderTimeoutPerRequest = defaultTTL
	}

	transport := etcd.DefaultTransport
	if options.Cert != "" && options.Key != "" {
		tlsCert, err := tls.LoadX509KeyPair(options.Cert, options.Key)
		if err != nil {
			return nil, err
		}
		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
		}
		if caCertCt, err := ioutil.ReadFile(options.CACert); err == nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCertCt)
			tlsCfg.RootCAs = caCertPool
		}
		transport = &http.Transport{
			TLSClientConfig: tlsCfg,
			Dial: func(network, address string) (net.Conn, error) {
				return (&net.Dialer{
					Timeout:   options.DialTimeout,
					KeepAlive: options.DialKeepAlive,
				}).Dial(network, address)
			},
		}
	}

	ce, err := etcd.New(etcd.Config{
		Endpoints:               machines,
		Transport:               transport,
		HeaderTimeoutPerRequest: options.HeaderTimeoutPerRequest,
	})
	if err != nil {
		return nil, err
	}

	return &client{
		keysAPI: etcd.NewKeysAPI(ce),
		ctx:     ctx,
	}, nil
}

// GetEntries implements the etcd Client interface.
func (c *client) GetEntries(key string) ([]string, error) {
	resp, err := c.keysAPI.Get(c.ctx, key, &etcd.GetOptions{Recursive: true})
	if err != nil {
		return nil, err
	}

	// Special case. Note that it's possible that len(resp.Node.Nodes) == 0 and
	// resp.Node.Value is also empty, in which case the key is empty and we
	// should not return any entries.
	if len(resp.Node.Nodes) == 0 && resp.Node.Value != "" {
		return []string{resp.Node.Value}, nil
	}

	entries := make([]string, len(resp.Node.Nodes))
	for i, node := range resp.Node.Nodes {
		entries[i] = node.Value
	}
	return entries, nil
}

// WatchPrefix implements the etcd Client interface.
func (c *client) WatchPrefix(prefix string, ch chan struct{}) {
	watch := c.keysAPI.Watcher(prefix, &etcd.WatcherOptions{AfterIndex: 0, Recursive: true})
	ch <- struct{}{} // make sure caller invokes GetEntries
	for {
		if _, err := watch.Next(c.ctx); err != nil {
			return
		}
		ch <- struct{}{}
	}
}
