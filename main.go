package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/coreos/go-systemd/v22/activation"
)

var (
	netType        = "tcp"
	netAddress     string
	exitIdleTime   = time.Second * 60
	connectionsMax int
	retryCount     = 30
)

func main() {
	flag.StringVar(&netType, "type", netType, "network type: tcp or unix")
	flag.StringVar(&netAddress, "address", netAddress, "proxy to the address")
	flag.IntVar(&connectionsMax, "connections-max", connectionsMax, "Set the maximum number of connections to be accepted")
	flag.DurationVar(&exitIdleTime, "exit-idle-time", exitIdleTime, "Exit when without a connection for this duration")
	flag.IntVar(&retryCount, "retry", retryCount, "retry reconnect upstream")
	flag.Parse()
	if len(netAddress) == 0 {
		flag.PrintDefaults()
		return
	}
	var l net.Listener
	listeners, err := activation.Listeners()
	if err != nil {
		panic(err)
	}
	if len(listeners) != 1 {
		panic("Unexpected number of socket activation fds")
	}
	l = listeners[0]
	exit := time.AfterFunc(exitIdleTime, func() { os.Exit(0) })
	var connectionCount int32
	for {
		c, err := l.Accept()
		if err != nil {
			log.Println(err)
			return
		}
		exit.Stop()
		if connectionsMax > 0 {
			if connectionCount > int32(connectionsMax) {
				log.Println("max connect")
				continue
			}
		}
		go func() {
			atomic.AddInt32(&connectionCount, 1)
			defer func() {
				atomic.AddInt32(&connectionCount, -1)
				if connectionCount == 0 {
					log.Println("exit idle")
					exit.Reset(exitIdleTime)
				}
			}()
			log.Println("proxy", c.RemoteAddr(), netType, netAddress)
			proxy(c, netType, netAddress)
			log.Println("proxy end", c.RemoteAddr(), netType, netAddress)
		}()
	}
}

func proxy(c net.Conn, netType, netAddress string) {
	defer c.Close()
	for i := 0; i < retryCount; i++ {
		s, err := net.Dial(netType, netAddress)
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue
		}
		defer s.Close()
		biCopy(context.Background(), c, s)
		break
	}
}

func biCopy(c context.Context, a, b net.Conn) {
	c, cancel := context.WithCancel(c)
	go func() {
		defer cancel()
		log.Println(io.Copy(a, b))
	}()
	go func() {
		defer cancel()
		log.Println(io.Copy(b, a))
	}()
	<-c.Done()
}
