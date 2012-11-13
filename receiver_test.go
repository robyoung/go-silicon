package silicon

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"testing"
	"time"
)

func server(address string, control chan bool) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Panicf("Failed to listen: %v", err)
	}
	control2 := make(chan bool)
	go listen(listener, control2)
	<-control
	control2 <- true
	listener.Close()
}

func listen(listener net.Listener, control2 chan bool) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v, %v", err, reflect.TypeOf(err))
			switch err.(type) {
			case *net.OpError:
				return
			}
		}
		go read(conn, control2)
	}
}

func read(conn net.Conn, control2 chan bool) {
	log.Printf("Found connection")
	reader := bufio.NewReader(conn)
	go readloop(reader)
	<-control2
	conn.Close()
}

func readloop(reader *bufio.Reader) {
	for {
		log.Println("Start of loop")
		line, prefix, err := reader.ReadLine()
		if prefix {
			err = fmt.Errorf("Line was too long")
		}
		if err != nil {
			log.Printf("Read error: %v", err)
			if err == io.EOF {
				return
			} else {
				continue
			}
		}
		metric, err := ParseLineMetric(string(line))
		if err != nil {
			log.Println(err)
		} else {
			log.Printf("Metric: %v", metric)
		}
		log.Printf("End of loop")
	}
}

func xTestLineReciever(t *testing.T) {
	address := ":2003"
	control := make(chan bool)

	go server(address, control)

	time.Sleep(1 * time.Second)

	//StartLineReceiver(address, control)

	//fmt.Println(address, control)
	conn, err := net.Dial("tcp", "127.0.0.1"+address)
	if err != nil {
		log.Panicf("Failed to connect: %v", err)
	}
	fmt.Fprintf(conn, "foo.bar.monkey 1234.56 74857843\n")
	time.Sleep(1 * time.Second)
	fmt.Fprintf(conn, "fooooooooooooooooooooooooooooooooooooooo 123.456 1234567\n")
	time.Sleep(1 * time.Second)
	fmt.Fprintf(conn, "ooooooooooo 123.456a 1234567\n")
	conn.Close()
	time.Sleep(500 * time.Millisecond)
	control <- true
	time.Sleep(1000 * time.Millisecond)

}
