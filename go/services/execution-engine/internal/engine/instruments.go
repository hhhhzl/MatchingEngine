package engine

import (
	"os"

	"gopkg.in/yaml.v3"
)

// InstrumentConfig provides tick/lot constraints required by the micro-decision library.
type InstrumentConfig struct {
	Symbol string  `yaml:"symbol"`
	Venue  string  `yaml:"venue"`
	TickSize float64 `yaml:"tick_size"`
	LotSize  float64 `yaml:"lot_size"`
	MinNotional float64 `yaml:"min_notional"`
}

type InstrumentsFile struct {
	Instruments []InstrumentConfig `yaml:"instruments"`
}

type instrumentIndex struct {
	m map[string]InstrumentConfig // key: venue|symbol
}

func loadInstruments(path string) (*instrumentIndex, error) {
	if path == "" {
		return &instrumentIndex{m: map[string]InstrumentConfig{}}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f InstrumentsFile
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return nil, err
	}
	idx := &instrumentIndex{m: map[string]InstrumentConfig{}}
	for _, it := range f.Instruments {
		idx.m[key2(it.Venue, it.Symbol)] = it
	}
	return idx, nil
}

func (i *instrumentIndex) get(venue, symbol string) InstrumentConfig {
	if i == nil {
		return InstrumentConfig{Symbol: symbol, Venue: venue, TickSize: 0.01, LotSize: 1, MinNotional: 0}
	}
	if it, ok := i.m[key2(venue, symbol)]; ok {
		if it.TickSize <= 0 {
			it.TickSize = 0.01
		}
		if it.LotSize <= 0 {
			it.LotSize = 1
		}
		return it
	}
	return InstrumentConfig{Symbol: symbol, Venue: venue, TickSize: 0.01, LotSize: 1, MinNotional: 0}
}

func key2(venue, symbol string) string {
	return venue + "|" + symbol
}

