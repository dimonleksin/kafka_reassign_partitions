package pkg

// Searching equals topics from broker and list with all topics
func search(m map[int]string, k string) bool {
	for _, v := range m {
		if k == v[0:len(v)-2] {
			return true
		}
	}
	return false
}

// return true if one of replicas contains in broker id from --from key
func searchForMove(m map[int32][]int32, key int32) bool {
	for _, v := range m {
		for i := 0; i < len(v); i++ {
			if v[i] == key {
				return true
			}
		}
	}
	return false
}
