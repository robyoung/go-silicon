package silicon

import (
	"github.com/robyoung/go-whisper"
	"os"
	"testing"
	"time"
	// "reflect"
	"fmt"
)

type dummyResolver struct {
}

func (r *dummyResolver) Find(key string) (whisper.Retentions, whisper.AggregationMethod, float32) {
	retentions, _ := whisper.ParseRetentionDefs("1s:5m,1m:30m")
	return retentions, whisper.Sum, 0.5
}

func makeGoodPoints(count, step int) []DataPoint {
	points := make([]DataPoint, count)
	now := int(time.Now().Unix())
	for i := 0; i < count; i++ {
		points[i] = DataPoint{100, now - (i * step)}
	}
	return points
}

func setUp() (string, string, StorageResolver) {
	return "/tmp/storage", "/tmp/storage/foo/bar.wsp", new(dummyResolver)
}

func setUpAndCheck(t *testing.T) (path, fullPath string, resolver StorageResolver) {
	path, fullPath, resolver = setUp()
	assertFileNotExists(t, path)

	return
}

func tearDown(path string) {
	os.RemoveAll(path)
}

func assertFileNotExists(t *testing.T, path string) {
	_, err := os.Stat(path)
	if err == nil || !os.IsNotExist(err) {
		tearDown(path)
		t.Fatalf("Path already exists: %v", err)
	}
}

func assertFetchedResults(t *testing.T, result *whisper.TimeSeries, count, value int) {
	points := result.Points()
	if length := len(points); length != 10 {
		t.Fatalf("Invalid number of points found")
	}
	for i, point := range points {
		if point.Value != 100 {
			t.Fatalf("Invalid value in point %v, %v", i, point.Value)
		}
	}
}

func TestCreatingSyncWriter(t *testing.T) {
	path, fullPath, resolver := setUpAndCheck(t)
	defer tearDown(path)

	writer := NewSyncWriter(path, resolver)

	now := int(time.Now().Unix())
	writer.Send("foo.bar", makeGoodPoints(10, 1))
	writer.Close()

	file, err := whisper.Open(fullPath)
	if err != nil {
		t.Fatalf("Error opening whisper file: %v", err)
	}

	result, err := file.Fetch(now-10, now)

	assertFetchedResults(t, result, 10, 100)
}

func TestCreatingAsyncWriter(t *testing.T) {
	path, fullPath, resolver := setUpAndCheck(t)
	defer tearDown(path)

	writer := NewAsyncWriter(path, resolver)

	now := int(time.Now().Unix())
	writer.Send("foo.bar", makeGoodPoints(10, 1))

	file, err := whisper.Open(fullPath)
	if err == nil {
		t.Fatalf("Error file should not exist yet: %v", err)
	}

	writer.Close()

	file, err = whisper.Open(fullPath)
	if err != nil {
		t.Fatalf("Error opening whisper file: %v", err)
	}

	result, err := file.Fetch(now-10, now)

	assertFetchedResults(t, result, 10, 100)
}

func TestParallelSyncWriter(t *testing.T) {
	path, fullPath, resolver := setUpAndCheck(t)
	defer tearDown(path)

	writer := NewParallelSyncWriter(path, resolver)

	now := int(time.Now().Unix())
	writer.Send("foo.bar", makeGoodPoints(10, 1))
	writer.Close()

	file, err := whisper.Open(fullPath)
	if err != nil {
		t.Fatalf("Error opening whisper file: %v", err)
	}

	result, err := file.Fetch(now-10, now)

	assertFetchedResults(t, result, 10, 100)
}

func Test_getFullPath(t *testing.T) {
	if path := getFullPath("/tmp", "foo.bar"); "/tmp/foo/bar.wsp" != path {
		t.Fatalf("Unexpected path: /tmp/foo/bar.wsp != %v", path)
	}
	if path := getFullPath("/tmp/", "foo.bar"); "/tmp/foo/bar.wsp" != path {
		t.Fatalf("Unexpected path: /tmp/foo/bar.wsp != %v", path)
	}
}

func benchmarkWriter(b *testing.B, makeWriter func(string, StorageResolver) Writer) {
	path, _, resolver := setUp()
	for i := 0; i < b.N; i++ {
		writer := makeWriter(path, resolver)
		done := make(chan bool)
		routines, times, keys := 10, 100, 100
		for i := 0; i < routines; i++ {
			go func() {
				for j := 0; j < times; j++ {
					for k := 0; k < keys; k++ {
						writer.Send(fmt.Sprintf("foo.bar%v", k), makeGoodPoints(10, 1))
					}
				}
				done <- true
			}()
		}
		for i := 0; i < routines; i++ {
			<-done
		}
		writer.Close()
		tearDown(path)
	}
}

func BenchmarkWriterSync(b *testing.B) {
	benchmarkWriter(b, func(path string, resolver StorageResolver) Writer {
		return NewSyncWriter(path, resolver)
	})
}

func BenchmarkWriterAsync(b *testing.B) {
	benchmarkWriter(b, func(path string, resolver StorageResolver) Writer {
		return NewAsyncWriter(path, resolver)
	})
}

func BenchmarkWriterPSync(b *testing.B) {
	benchmarkWriter(b, func(path string, resolver StorageResolver) Writer {
		return NewParallelSyncWriter(path, resolver)
	})
}