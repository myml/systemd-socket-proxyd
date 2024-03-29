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
	flag.StringVar(&netType, "type", netType, "网络类型: tcp 或 unix")
	flag.StringVar(&netAddress, "address", netAddress, "上游地址")
	flag.IntVar(&connectionsMax, "connections-max", connectionsMax, "允许最大连接数量")
	flag.DurationVar(&exitIdleTime, "exit-idle-time", exitIdleTime, "在空闲指定时间后退出")
	flag.IntVar(&retryCount, "retry", retryCount, "重试连接上游的次数")
	flag.Parse()
	if len(netAddress) == 0 {
		flag.PrintDefaults()
		return
	}
	var l net.Listener
	listeners, err := activation.Listeners()
	if err != nil {
		log.Panic("Listeners", err)
	}
	if len(listeners) != 1 {
		log.Panic("Unexpected number of socket activation fds")
	}
	l = listeners[0]
	idleTimer := time.AfterFunc(exitIdleTime, func() { os.Exit(0) })
	var connectionCount int32
	for {
		c, err := l.Accept()
		if err != nil {
			log.Panic("Accept", err)
		}
		if connectionsMax > 0 && connectionCount > int32(connectionsMax) {
			c.Close()
			log.Println("max connect limit")
			continue
		}
		idleTimer.Stop()
		go func() {
			defer c.Close()
			atomic.AddInt32(&connectionCount, 1)
			defer func() {
				atomic.AddInt32(&connectionCount, -1)
				if connectionCount == 0 {
					log.Printf("连接空闲，服务在 %s 后退出\n", exitIdleTime)
					idleTimer.Reset(exitIdleTime)
				}
			}()
			for i := 1; i < retryCount; i++ {
				s, err := net.DialTimeout(netType, netAddress, time.Second)
				if err != nil {
					log.Printf("无法连接到上游(重试次数%d/%d): %s\n", i, retryCount, err)
					time.Sleep(time.Second)
					continue
				}
				defer s.Close()
				log.Println("开始转发", c.RemoteAddr(), "<=>", netAddress)
				err = biCopy(context.Background(), c, s)
				log.Println("结束转发", c.RemoteAddr(), "<=>", netAddress, err)
				return
			}
		}()
	}
}

func biCopy(c context.Context, a, b net.Conn) error {
	c, cancel := context.WithCancelCause(c)
	go func() {
		_, err := io.Copy(a, b)
		cancel(err)
	}()
	go func() {
		_, err := io.Copy(b, a)
		cancel(err)
	}()
	<-c.Done()
	return context.Cause(c)
}
