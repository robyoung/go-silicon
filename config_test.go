package silicon

import (
	"github.com/robyoung/go-whisper"
	"testing"
)

func TestFileStorageResolver(t *testing.T) {
	resolver, err := NewFileStorageResolver("config/storage-schemas.conf", "config/storage-aggregation.conf")
	if err != nil {
		t.Fatalf("Error reading storage schemas: %v", err)
	}
	retentions, aggregationMethod, xFilesFactor, err := resolver.Find("carbon.foo")
	if len(retentions) != 1 {
		t.Fatalf("Expecting 2 retentions")
	}
	if aggregationMethod != whisper.Average {
		t.Fatalf("Expecting aggregation method to be average")
	}
	if xFilesFactor != 0.5 {
		t.Fatalf("Expecting xFilesFactor to be 0,5")
	}
}
