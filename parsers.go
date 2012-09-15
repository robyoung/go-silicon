package silicon

import (
	"fmt"
	"strconv"
	"strings"
)

func ParseLineMetric(line string) (*Metric, error) {
	parts := strings.Split(line, " ")
	if len(parts) != 3 {
		return nil, fmt.Errorf("Cannot parse metric, invalid number of parts '%v'", line)
	}
	value, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse metric, invalid value '%v'", parts[1])
	}
	timestamp, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse metric, invalid timestamp '%v'", parts[2])
	}

	return &Metric{parts[0], value, int(timestamp)}, nil
}
