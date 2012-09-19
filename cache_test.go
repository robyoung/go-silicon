package silicon

import (
	"fmt"
	"testing"
	"time"
)

func metric(key string) *Metric {
  return &Metric{key, DataPoint{1234, 123456}}
}

func TestStore(t *testing.T) {
  cache := NewMetricCache()
  cache.Store(metric("foo.bar"))
  if cache.Size() != 0 {
    t.Fatalf("Expecting metric to take some time to be visible")
  }
  time.Sleep(15 * time.Microsecond)
  if cache.Size() != 1 {
    t.Fatalf("Expecting metric to eventually be visible")
  }
}

func TestPop(t *testing.T) {
  var result []DataPoint
  cache := NewMetricCache()
  result = cache.Pop("foo.bar")
  if len(result) != 0 {
    t.Fatalf("Expecting Pop result to be empty before Store")
  }
  cache.Store(metric("foo.bar"))
  time.Sleep(5 * time.Microsecond)
  result = cache.Pop("foo.bar")
  if len(result) != 1 {
    t.Fatalf("Expecting Pop result to have one item in before Store")
  }
  if cache.Size() != 0 {
    t.Fatalf("Expecting Pop to reduce Size")
  }
}

func TestCounts(t *testing.T) {
  cache := NewMetricCache()
  cache.Store(metric("foo.bar"))
  cache.Store(metric("foo.bar"))
  cache.Store(metric("foo.bar"))
  cache.Store(metric("foo.baz"))
  cache.Store(metric("foo.baz"))
  time.Sleep(20 * time.Microsecond)

  if size := cache.Size(); size != 5 {
    t.Fatalf("Expecting Size to be 5, received %v", size)
  }
  counts := cache.Counts()
  if length := len(counts); length != 2 {
    t.Fatalf("Expecting Counts length to be 2, received %v", length)
  }
  if count, ok := counts["foo.bar"]; ok != true || count != 3 {
    if ok != true {
      t.Fatalf("Expecting count of three for 'foo.bar' but none was found")
    } else {
      t.Fatalf("Expecting count of three for 'foo.bar' but received %v", count)
    }
  }
}

func TestClose(t *testing.T) {
  cache := NewMetricCache()
  cache.Store(metric("foo.bar"))
  cache.Store(metric("foo.bar"))
  cache.Store(metric("foo.bar"))
  cache.Store(metric("foo.baz"))
  cache.Store(metric("foo.baz"))
  time.Sleep(5 * time.Microsecond)

  result := cache.Close()
  if length := len(result); length != 2 {
    t.Fatalf("Expecting a length of 2 but received %v", length)
  }
  defer func() {
    if r := recover(); r == nil {
      t.Fatalf("Expecting a panic if Store is called on the closed cache")
    }
  }()
  cache.Store(metric("foo.bar"))
}


func benchmarkMetricCache(b *testing.B, cache MetricCache) {
	// create a finished channel
	finishedFill := make(chan bool)
	finishedPop := make(chan bool)
	closed := make(chan bool)
  var routines, times, keys int = 3, 1000, 100
	for j := 0; j < routines; j++ {
		go func() {
			for j := 0; j < times; j++ {
				for k := 0; k < keys; k++ {
					cache.Store(&(Metric{fmt.Sprintf("foo.%v", k), DataPoint{123.234, 123456}}))
				}
			}
			finishedFill <- true
		}()
	}
	go func() {
		for {
			select {
			case <-finishedPop:
				closed <- true
				return
			default:
				max := 0
				var maxKey string
				for key, count := range cache.Counts() {
					if count > max && key != "foo.4" {
						max = count
						maxKey = key
					}
				}
				if maxKey != "" {
					cache.Pop(maxKey)
				}
			}
		}
	}()
	// wait until all have finished
	for j := 0; j < routines; j++ {
		<-finishedFill
	}
	finishedPop <- true
	<-closed
	// run a couple of queries
	result := cache.Pop("foo.4")
	if len(result) != (routines * times) {
		b.Fatalf("Failed test %v", len(result))
	}
	cache.Close()
}

func BenchmarkMetricCache(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkMetricCache(b, NewMetricCache())
	}
}
