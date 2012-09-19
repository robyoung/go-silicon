package silicon

import (
	"fmt"
)

/*
  Describes a metric, this is made up of a dot separated key (eg. machine.stats.cpu), a unix timestamp and a numeric value.
*/
type Metric struct {
	key string
	DataPoint
}

func (metric *Metric) String() string {
	return fmt.Sprintf("Metric{key: %v, value: %v, timestamp: %v}", metric.key, metric.value, metric.timestamp)
}

/*
  Describes a numeric value at a point in time. It is made up of a value and a unix timestamp.
*/
type DataPoint struct {
	value     float64
	timestamp int
}
