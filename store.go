/*
	Package implements a storage writer to manage sending data points to the data store.
*/
package silicon

import (
	"fmt"
	"github.com/robyoung/go-cache"
	"github.com/robyoung/go-whisper"
	"log"
	"os"
	"path"
	"strings"
)

/*
	Interface for writing data points to a backend store.
*/
type Writer interface {
	/*
		Send the slice of DataPoints to the data store against the given key.
	*/
	Send(string, []DataPoint)

	/*
		Close the writer and wait for all writes to complete.
	*/
	Close()
}

/*
	Retrieve Whisper configuration details for a given key.
*/
type StorageResolver interface {
	Find(string) (whisper.Retentions, whisper.AggregationMethod, float32, error)
}

type writer struct {
	basePath string
	resolver StorageResolver
	in       chan *storageMessage
	done     chan bool
}

type storageMessage struct {
	key    string
	points []DataPoint
}

/*
	Stores metadata about an open whisper file.
*/
type writeMetadata struct {
	whisper *whisper.Whisper
	in      chan *storageMessage
	done    chan bool
}

/*
	Create a new writer that creates files under `basePath` and fetches
	Whisper configuration details from resolver.
*/
func NewWriter(basePath string, resolver StorageResolver) *writer {
	w := new(writer)
	w.basePath = basePath
	w.resolver = resolver
	w.in = make(chan *storageMessage)
	w.done = make(chan bool)

	go w.run()

	return w
}

/*
	Send a set of data points to the whisper file identified by the key.
*/
func (w *writer) Send(key string, points []DataPoint) {
	w.in <- &storageMessage{key, points}
}

/*
	Close the writer, wait for all messages to be written and files to be closed.
*/
func (w *writer) Close() {
	close(w.in)
	<-w.done
}

func (w *writer) run() {
	cache := w.createCache()
	for message := range w.in {
		value, _ := cache.Get(message.key)
		var metadata *writeMetadata
		if value != nil {
			metadata = value.(*writeMetadata)
		} else {
			// TODO: consider moving this inside runWriter
			file, err := w.createWhisper(message.key)
			if err != nil {
				log.Printf("Failed to create Whisper: %v", err)
				metadata = &writeMetadata{}
			} else {
				metadata = &writeMetadata{file, make(chan *storageMessage), make(chan bool)}
			}
			cache.Set(message.key, metadata)
			go w.runWriter(metadata)
		}
		if metadata != nil {
			metadata.in <- message
		}
	}
	for _, key := range cache.Keys() {
		// manually delete to cause the evictions to run
		cache.Delete(key)
	}
	w.done <- true
}

func (w *writer) createCache() *cache.LRUCache {
	c := cache.NewLRUCache(50) // TODO: make cache size configurable
	c.SetEvictionHook(func(key string, value interface{}) {
		metadata := value.(*writeMetadata)
		close(metadata.in)
		<-metadata.done
		metadata.whisper.Close()
	})

	return c
}

func (w *writer) createWhisper(key string) (*whisper.Whisper, error) {
	retentions, aggregationMethod, xFilesFactor, err := w.resolver.Find(key)
	if err != nil {
		return nil, fmt.Errorf("Resolver error: %v", err)
	}
	fullPath := w.getFullPath(key)
	os.MkdirAll(path.Dir(fullPath), os.ModeDir|os.ModePerm)

	file, err := whisper.Create(fullPath, retentions, aggregationMethod, xFilesFactor)
	if err != nil {
		if err == os.ErrExist {
			file, err = whisper.Open(fullPath)
		}
	}

	return file, fmt.Errorf("Create error: %v", err)
}

func (w *writer) getFullPath(key string) string {
	return path.Join(w.basePath, strings.Replace(key, ".", "/", -1)+".wsp")
}

func (w *writer) runWriter(metadata *writeMetadata) {
	for message := range metadata.in {
		metadata.whisper.UpdateMany(toTimeSeries(message.points))
	}
	metadata.done <- true
}

func toTimeSeries(points []DataPoint) []*whisper.TimeSeriesPoint {
	result := make([]*whisper.TimeSeriesPoint, len(points))
	for i, point := range points {
		result[i] = &whisper.TimeSeriesPoint{point.timestamp, point.value}
	}
	return result
}
