package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/devopsfaith/krakend-etcd"
	"github.com/devopsfaith/krakend/config"
	"github.com/devopsfaith/krakend/logging"
	"github.com/devopsfaith/krakend/proxy"
	"github.com/devopsfaith/krakend/router"
	krakendgin "github.com/devopsfaith/krakend/router/gin"
	"github.com/gin-gonic/gin"
)

func main() {
	port := flag.Int("p", 0, "Port of the service")
	logLevel := flag.String("l", "ERROR", "Logging level")
	debug := flag.Bool("d", false, "Enable the debug")
	configFile := flag.String("c", "E:/megvii/project/go_learning/krakend-etcd/example/krakend.json", "Path to the configuration filename")
	flag.Parse()

	parser := config.NewParser()
	serviceConfig, err := parser.Parse(*configFile)
	if err != nil {
		log.Fatal("ERROR:", err.Error())
	}
	serviceConfig.Debug = serviceConfig.Debug || *debug
	if *port != 0 {
		serviceConfig.Port = *port
	}

	logger, err := logging.NewLogger(*logLevel, os.Stdout, "[KRAKEND]")
	if err != nil {
		log.Fatal("ERROR:", err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())

	etcdClient, err := etcd.New(ctx, serviceConfig.ExtraConfig)
	if err != nil {
		log.Fatal("ERROR:", err.Error())
	}

	routerFactory := krakendgin.NewFactory(krakendgin.Config{
		Engine:         gin.Default(),
		Middlewares:    []gin.HandlerFunc{},
		Logger:         logger,
		HandlerFactory: krakendgin.EndpointHandler,
		ProxyFactory: customProxyFactory{
			logger,
			proxy.DefaultFactoryWithSubscriber(logger, etcd.SubscriberFactory(ctx, etcdClient)),
		},
		RunServer: router.RunServer,
	})

	routerFactory.NewWithContext(ctx).Run(serviceConfig)

	cancel()
}

// customProxyFactory adds a logging middleware wrapping the internal factory
type customProxyFactory struct {
	logger  logging.Logger
	factory proxy.Factory
}

// New implements the Factory interface
func (cf customProxyFactory) New(cfg *config.EndpointConfig) (p proxy.Proxy, err error) {
	p, err = cf.factory.New(cfg)
	if err == nil {
		p = proxy.NewLoggingMiddleware(cf.logger, cfg.Endpoint)(p)
	}
	return
}
