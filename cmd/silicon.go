package main

import (
	"github.com/robyoung/go-silicon"
	"fmt"
	"net"
	"os/signal"
	"os"
)

func main() {
	metricCache := silicon.NewMetricCache()
	storageResolver, err := silicon.NewFileStorageResolver("config/storage-schemas.conf", "config/storage-aggregation.conf")
	storageWriter := silicon.NewWriter("./db", storageResolver)
	cacheBolt := silicon.NewCacheBolt(metricCache, storageWriter)
	listener, err := net.Listen("tcp", ":2003")
	if err != nil {
		fmt.Printf("Failed to listen: %v", err)
	}
	receiver := silicon.NewMetricReceiver(listener, metricCache)

	fmt.Println(receiver, cacheBolt)

	interrupted := make(chan os.Signal)
	signal.Notify(interrupted, os.Kill)
	<-interrupted
}
