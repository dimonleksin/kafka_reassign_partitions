package pkg

import (
	"fmt"
	"log"
)

// Deleting zero value from plane
func clearZeroValue(plane map[string][][]int32) (clearPlane map[string][][]int32, err error) {
	var (
		tmp map[string]map[int]map[int32]int32
	)
	log.Println("Clearing plane from zero value")

	tmp, err = makeMapFromPlane(plane)
	if err != nil {
		return nil, err
	}
	clearPlane = make(map[string][][]int32)

	for t, p := range tmp {
		if len(clearPlane[t]) == 0 {
			clearPlane[t] = make([][]int32, len(p))
		}
		for partition, brokers := range p {
			if len(clearPlane[t][partition]) == 0 {

				clearPlane[t][partition] = make([]int32, len(brokers))

			}
			for k, b := range brokers {
				if b != 0 {
					clearPlane[t][partition][k] = b
				}
			}
		}
	}
	return clearPlane, nil
}

func makeMapFromPlane(plane map[string][][]int32) (mapWithoutThero map[string]map[int]map[int32]int32, err error) {
	var (
		c int32
	)
	if len(plane) == 0 {
		return nil, fmt.Errorf("plane without values")
	}

	mapWithoutThero = make(map[string]map[int]map[int32]int32)

	for t, p := range plane {
		if len(mapWithoutThero[t]) == 0 {
			mapWithoutThero[t] = make(map[int]map[int32]int32)
		}

		if len(p) != 0 {
			for partition, brokers := range p {
				if len(brokers) != 0 {
					c = 0
					if len(mapWithoutThero[t][partition]) == 0 {
						mapWithoutThero[t][partition] = make(map[int32]int32)
					}

					for _, b := range brokers {
						if b != 0 {
							mapWithoutThero[t][partition][c] = b
							c++
						}
					}
				}
			}
		}
	}
	return mapWithoutThero, nil
}
