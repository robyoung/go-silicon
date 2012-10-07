package silicon

import (
	"github.com/robyoung/go-whisper"
	"github.com/robyoung/go-cache"
	"os"
	"path"
	"strings"
)

type Writer interface {
	Send(string, []DataPoint)
	Close()
}

type StorageResolver interface {
	Find(string) (whisper.Retentions, whisper.AggregationMethod, float32)
}

type storageMessage struct {
	key    string
	points []DataPoint
	result chan bool
}

type syncWriter struct {
	basePath string
	resolver StorageResolver
	in       chan *storageMessage
	done     chan bool
}

func NewSyncWriter(basePath string, resolver StorageResolver) *syncWriter {
	writer := new(syncWriter)
	writer.basePath = basePath
	writer.resolver = resolver
	writer.in = make(chan *storageMessage)
	writer.done = make(chan bool)

	go writer.run()

	return writer
}

func (writer *syncWriter) Send(key string, points []DataPoint) {
	out := make(chan bool)
	writer.in <- &storageMessage{key, points, out}
	<-out
}

func (writer *syncWriter) Close() {
	close(writer.in)
	<-writer.done
}

func (writer *syncWriter) run() {
	runSerial(writer.in, writer.done, writer.resolver, writer.basePath)
}

type asyncWriter struct {
	basePath string
	resolver StorageResolver
	in       chan *storageMessage
	done     chan bool
}

func NewAsyncWriter(basePath string, resolver StorageResolver) *asyncWriter {
	writer := new(asyncWriter)
	writer.basePath = basePath
	writer.resolver = resolver
	writer.in = make(chan *storageMessage)
	writer.done = make(chan bool)

	go writer.run()

	return writer
}

func (writer *asyncWriter) Send(key string, points []DataPoint) {
	writer.in <- &storageMessage{key, points, nil}
}

func (writer *asyncWriter) Close() {
	close(writer.in)
	<-writer.done
}

func (writer *asyncWriter) run() {
	runSerial(writer.in, writer.done, writer.resolver, writer.basePath)
}

func runSerial(in <-chan *storageMessage, done chan<- bool, resolver StorageResolver, basePath string) {
	for message := range in {
		file, err := getWhisper(message.key, basePath, resolver)		
		if err == nil {
			timeSeries := toTimeSeries(message.points)

			file.UpdateMany(timeSeries)

			file.Close()
		} else {
			// TODO: log the error
		}

		if message.result != nil {
			message.result <- true
		}
	}
	done <- true
}

// TODO: remove duplication with composition
type parallelSyncWriter struct {
	basePath string
	resolver StorageResolver
	in       chan *storageMessage
	done     chan bool
}

func NewParallelSyncWriter(basePath string, resolver StorageResolver) *parallelSyncWriter {
	writer := new(parallelSyncWriter)
	writer.basePath = basePath
	writer.resolver = resolver
	writer.in = make(chan *storageMessage)
	writer.done = make(chan bool)

	go writer.run()

	return writer
}

func (writer *parallelSyncWriter) Send(key string, points []DataPoint) {
	out := make(chan bool)
	writer.in <- &storageMessage{key, points, out}
	<-out
}

func (writer *parallelSyncWriter) Close() {
	close(writer.in)
	<-writer.done
}

func (writer *parallelSyncWriter) run() {
	runParallel(writer.in, writer.done, writer.resolver, writer.basePath)
}

type whisperChannel struct {
	// TODO: do we need the whisper reference here?
	whisper *whisper.Whisper
	in chan *storageMessage
	done chan bool
}

func createWhisperChannel()

func runParallel(in <-chan *storageMessage, done chan<- bool, resolver StorageResolver, basePath string) {
	c := cache.NewLRUCache(50)
	c.SetEvictionHook(func(key string, value interface{}) {
		item := value.(*whisperChannel)
		close(item.in)
		<-item.done
		item.whisper.Close()
	})
	for message := range in {
		value, _ := c.Get(message.key)
		var item *whisperChannel
		if value != nil {
			item = value.(*whisperChannel)
		} else {
			// create a new
			file, err := getWhisper(message.key, basePath, resolver)
			if err != nil {
				item = &whisperChannel{}
			} else {
				item = &whisperChannel{file, make(chan *storageMessage), make(chan bool)}
			}
			c.Set(message.key, item)
			go func(item *whisperChannel) {
				for message := range item.in {
					item.whisper.UpdateMany(toTimeSeries(message.points))
					if message.result != nil {
						message.result <- true
					}
				}
				item.done<- true
			}(item)
		}
		if item != nil {
			item.in <- message
		}
	}
	done<-true
}

func getFullPath(basePath, key string) string {
	return strings.TrimRight(basePath, "/") + "/" + strings.Replace(key, ".", "/", -1) + ".wsp"
}

func toTimeSeries(points []DataPoint) []*whisper.TimeSeriesPoint {
	result := make([]*whisper.TimeSeriesPoint, len(points))
	for i, point := range points {
		result[i] = &whisper.TimeSeriesPoint{point.timestamp, point.value}
	}
	return result
}

func getWhisper(key, basePath string, resolver StorageResolver) (*whisper.Whisper, error) {
	retentions, aggregationMethod, xFilesFactor := resolver.Find(key)
	fullPath := getFullPath(basePath, key)
	os.MkdirAll(path.Dir(fullPath), os.ModeDir|os.ModePerm)

	file, err := whisper.Create(fullPath, retentions, aggregationMethod, xFilesFactor)
	if err != nil {
		if err == os.ErrExist {
			file, err = whisper.Open(fullPath)
		}
	}
	return file, err
}