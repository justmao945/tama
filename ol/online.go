package ol

type Item struct {
	On  int
	Off int
}

func CountOnline(data []Item) (base int, ret []int) {
	min := data[0].On
	max := data[0].Off
	for _, v := range data {
		if min > v.On {
			min = v.On
		}
		if max < v.Off {
			max = v.Off
		}
	}
	duration := max - min + 1
	base = min

	ret = make([]int, duration)
	chg := make([]int, duration)
	for _, v := range data {
		chg[v.On-min]++
		chg[v.Off-min]--
	}
	ret[0] = chg[0]
	for i := 1; i < duration; i++ {
		ret[i] = ret[i-1] + chg[i]
	}
	return
}
