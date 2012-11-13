package silicon

import (
	"fmt"
	"math"
	"time"
)

/*
  A concurrent metric cache. It should be fast to store metrics and should not block.

*/
type MetricCache interface {
	Store(*Metric)                 // non-blocking, eventually delivered
	Size() int                     // the total number of data points on all keys
	Pop(string) []DataPoint        // remove and return all data points for a given key
	Counts() map[string]int        // return a map of keys and their counts
	Close() map[string][]DataPoint // close down this metric cache
}

type metricCache struct {
	data     map[string][]DataPoint
	count    int
	commands chan commandData
}

func NewMetricCache() *metricCache {
	cache := new(metricCache)
	cache.data = make(map[string][]DataPoint)
	cache.commands = make(chan commandData, 10)
	go cache.run()
	return cache
}

type commandData struct {
	action commandAction
	key    string
	value  interface{}
	result chan<- interface{}
}

type commandAction int

const (
	store commandAction = iota
	size
	pop
	counts
	end
)

func (cache *metricCache) Store(metric *Metric) {
	cache.commands <- commandData{action: store, value: metric}
}

func (cache *metricCache) Size() int {
	result := make(chan interface{})
	cache.commands <- commandData{action: size, result: result}
	return (<-result).(int)
}

func (cache *metricCache) Pop(key string) []DataPoint {
	result := make(chan interface{})
	cache.commands <- commandData{action: pop, value: key, result: result}
	return (<-result).([]DataPoint)
}

func (cache *metricCache) Counts() map[string]int {
	result := make(chan interface{})
	cache.commands <- commandData{action: counts, result: result}
	return (<-result).(map[string]int)
}

func (cache *metricCache) Close() (data map[string][]DataPoint) {
	result := make(chan interface{})
	cache.commands <- commandData{action: end, result: result}
	close(cache.commands)
	<-result
	return cache.data
}

func (cache *metricCache) run() {
	for command := range cache.commands {
		switch command.action {
		case store:
			metric := (command.value).(*Metric)
			cache.data[metric.key] = append(cache.data[metric.key], metric.DataPoint)
			cache.count++
		case size:
			command.result <- cache.count
		case pop:
			result, found := cache.data[(command.value).(string)]
			if found {
				delete(cache.data, (command.value).(string))
			}
			cache.count -= len(result)
			command.result <- result
		case counts:
			result := make(map[string]int, len(cache.data))
			for key, datapoints := range cache.data {
				result[key] = len(datapoints)
			}
			command.result <- result
		case end:
			command.result <- true
			return
		}
	}
}

/*
	A cache bolt allows you to attach a MetricCache to a CacheSink.
	The bolt continuously polls the cache and pops metrics off in 
	use order.
*/
type cacheBolt struct {
	control chan bool
	cache   MetricCache
	sink    CacheSink
}

type CacheSink interface {
	Send(string, []DataPoint)
}

func NewCacheBolt(cache MetricCache, sink CacheSink) *cacheBolt {
	bolt := new(cacheBolt)
	bolt.control = make(chan bool)
	bolt.cache = cache
	bolt.sink = sink

	go bolt.run()

	return bolt
}

/*
	Is there a more idiomatic way of aproaching this? Some way
	of letting the cache signal that it has more data.
*/
func (bolt *cacheBolt) run() {
	backoff := 0.0
	backoffLimit := 100000.0
	for {
		counts := bolt.cache.Counts()
		if len(counts) == 0 {
			backoff += 1
			time.Sleep(time.Duration(math.Min(backoffLimit, math.Pow(backoff, 5))) * time.Millisecond)
		} else {
			fmt.Println(counts, len(counts), backoff)
			// sort the counts
			for key := range counts {
				points := bolt.cache.Pop(key)
				bolt.sink.Send(key, points)
			}
		}
	}
}
