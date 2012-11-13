package silicon

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
)

type Receiver struct {
	listener net.Listener
	cache    MetricCache
}

func NewMetricReceiver(listener net.Listener, cache MetricCache) *Receiver {
	receiver := &Receiver{listener, cache}
	go receiver.run()

	return receiver
}

func (receiver *Receiver) run() {
	for {
		conn, err := receiver.listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection %v", err)
		}
		go receiver.read(conn)
	}
}

func (receiver *Receiver) read(conn net.Conn) {
	fmt.Println("WE ARE READY TO READ")
	reader := bufio.NewReader(conn)
	for {
		line, prefix, err := reader.ReadLine()
		if prefix {
			err = fmt.Errorf("Line was too long")
		}
		if err != nil {
			if err == io.EOF {
				conn.Close()
				return
			} else {
				log.Printf("Metric read error: %v", err)
			}
		} else {
			metric, err := ParseLineMetric(string(line))
			if err != nil {
				log.Printf("Invalid metric: %v", err)
			} else {
				receiver.cache.Store(metric)
			}
		}
	}
}
