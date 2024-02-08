package pkg

import (
	"fmt"
	"log"
)

func clearZeroValue(plane map[string][][]int32) (clearPlane map[string][][]int32) {
	var (
		tmp map[string]map[int]map[int32]int32
		c   int32
	)
	log.Println("Clearing plane from zero value")
	fmt.Println("\n\n")

	//         [topic][partition][]asigns
	tmp = make(map[string]map[int]map[int32]int32)
	clearPlane = make(map[string][][]int32)

	for t, p := range plane {
		// log.Println(t, p)
		if len(tmp[t]) == 0 {
			tmp[t] = make(map[int]map[int32]int32)
		}

		if len(p) != 0 {
			// log.Println(p)
			for partition, brokers := range p {
				if len(brokers) != 0 {
					c = 0
					if len(tmp[t][partition]) == 0 {
						tmp[t][partition] = make(map[int32]int32)
					}

					// log.Printf("topic: %s, partition: %d, assign %v", t, partition, brokers)
					for _, b := range brokers {
						if b != 0 {
							tmp[t][partition][c] = b
							c++
						}
					}

				}
			}
		}
		// log.Println(tmp[t])
	}
	// log.Println(tmp)
	for t, p := range tmp {
		if len(clearPlane[t]) == 0 {
			clearPlane[t] = make([][]int32, len(p))
		}
		for partition, brokers := range p {
			if len(clearPlane[t][partition]) == 0 {
				clearPlane[t][partition] = make([]int32, len(brokers))
				// log.Println(len(brokers))
			}
			for k, b := range brokers {
				if b != 0 {
					clearPlane[t][partition][k] = b
				}
			}
		}
	}
	// log.Println(clearPlane)
	return clearPlane
}
