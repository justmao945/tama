package wildcard

import (
	"testing"
)

var items = [][]string{
	{"", "", "true"},
	{"abcd", "", "false"},
	{"", "*", "true"},
	{"", "?", "false"},
	{"aa", "a", "false"},
	{"aa", "aa", "true"},
	{"aaa", "aa", "false"},
	{"aa", "*", "true"},
	{"aa", "a*", "true"},
	{"ab", "?*", "true"},
	{"aab", "c*a*b", "false"},
	{"aab", "a??", "true"},
	{"*", "?", "true"},
	{"*", "*", "true"},
	{"?", "*", "true"},
	{"?", "?", "true"},
	{"*abcd", "*", "true"},
	{"abbbbbabbbazzzaccccaxxxxaddddaeeeafffaggggahhhaeeeeazzzzattt", "a*a*a*a*a*a*a*a*a*a*a*a*a*", "true"},
	{"abbbbbabbbazzzaccccaxxxxaddddaeeeafffaggggahhhaeeeeazzzzattt", "a***************************", "true"},
}

func Bool(b bool) string {
	if b {
		return "true"
	} else {
		return "false"
	}
}

func TestMatch(t *testing.T) {
	for _, item := range items {
		str, pat, res := item[0], item[1], item[2]
		matched := Match(str, pat)
		if Bool(matched) != res {
			t.Error(item)
		}
	}
}
