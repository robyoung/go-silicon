package silicon

import (
	"testing"
)

func TestParseLineMetric(t *testing.T) {
	metric, err := ParseLineMetric("foo.bar.baz 42 74857843")
	if err != nil {
		t.Fatalf("Failed to parse metric: %v", err)
	}
	if metric.key != "foo.bar.baz" {
		t.Fatalf("Invalid metric key. Expecting foo.bar.baz, received %v", metric.key)
	}
	if metric.value != 42 {
		t.Fatalf("Invalid metric value. Expecting 42, received %v", metric.value)
	}
	if metric.timestamp != 74857843 {
		t.Fatalf("Invalid metric timestamp. Expecting 74857843, received %v", metric.timestamp)
	}
}
