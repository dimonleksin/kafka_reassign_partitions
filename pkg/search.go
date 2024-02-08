package pkg

func search(m map[int]string, k string) bool {
	for _, v := range m {
		if k == v[0:len(v)-2] {
			return true
		}
	}
	return false
}
