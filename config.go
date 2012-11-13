package silicon

import (
	"fmt"
	"github.com/kless/goconfig/config"
	"github.com/robyoung/go-whisper"
	"regexp"
)

/*
storageSchemas, err := ParseStorageSchemas(path)
retentions, err := storageSchemas.Retentions(key)

carbonConf, err := ParseCarbonConf(path)
carbonConf.Get("CARBON_PATH")
*/

/*
	Retrieve Whisper configuration details for a given key from a file.
*/
type fileStorageResolver struct {
	schemas     *config.Config
	aggregation *config.Config
}

func NewFileStorageResolver(schemasPath, aggregationPath string) (*fileStorageResolver, error) {
	resolver := new(fileStorageResolver)
	schemas, err := config.ReadDefault(schemasPath)
	if err != nil {
		return nil, err
	}
	resolver.schemas = schemas
	aggregation, err := config.ReadDefault(aggregationPath)
	if err != nil {
		return nil, err
	}
	resolver.aggregation = aggregation

	return resolver, nil
}

func (resolver *fileStorageResolver) Find(key string) (retentions whisper.Retentions, aggregationMethod whisper.AggregationMethod, xFilesFactor float32, err error) {
	retentions, err = resolver.findRetentions(key)
	if err != nil {
		return nil, 0, 0, err
	}
	aggregationMethod, xFilesFactor, err = resolver.findAggregation(key)
	if err != nil {
		return nil, 0, 0, err
	}

	return
}

func (resolver *fileStorageResolver) findRetentions(key string) (whisper.Retentions, error) {
	for _, section := range resolver.schemas.Sections()[1:] {
		pattern, err := resolver.schemas.String(section, "pattern")
		if err == nil {
			if regexp.MustCompile(pattern).MatchString(key) {
				retentionDef, err := resolver.schemas.String(section, "retentions")
				if err != nil {
					return nil, err
				}
				return whisper.ParseRetentionDefs(retentionDef)
			}
		}
	}
	return nil, fmt.Errorf("Could not find retention defs for '%v'", key)
}

func (resolver *fileStorageResolver) findAggregation(key string) (whisper.AggregationMethod, float32, error) {
	for _, section := range resolver.aggregation.Sections()[1:] {
		pattern, err := resolver.aggregation.String(section, "pattern")
		if err == nil {
			if regexp.MustCompile(pattern).MatchString(key) {
				aggregationMethodS, err := resolver.aggregation.String(section, "aggregationMethod")
				if err != nil {
					return 0, 0, err
				}
				aggregationMethod, err := resolver.parseAggregationMethod(aggregationMethodS)
				if err != nil {
					return 0, 0, err
				}
				xFilesFactor, err := resolver.aggregation.Float(section, "xFilesFactor")
				if err != nil {
					return 0, 0, err
				}
				return aggregationMethod, float32(xFilesFactor), nil
			}
		}
	}
	return 0, 0, fmt.Errorf("Could not find aggregation defs for '%v'", key)
}

func (resolver *fileStorageResolver) parseAggregationMethod(aggregationMethod string) (whisper.AggregationMethod, error) {
	switch aggregationMethod {
	case "average":
		return whisper.Average, nil
	case "sum":
		return whisper.Sum, nil
	case "max":
		return whisper.Max, nil
	case "min":
		return whisper.Min, nil
	}
	return 0, fmt.Errorf("Invalid aggregation method '%v'", aggregationMethod)
}
