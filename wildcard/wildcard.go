// Package wildcard implements a matcher to do wildcard pattern matching.
package wildcard

// Match returns true if str is matched by pat entirely in O(N) time complexity.
// pat supports:
//	'?' Matches any single character
//	'*' Matches any sequence of characters, including empty
func Match(str, pat string) bool {
	var i, j int
	var star, pms int = -1, -1

	if len(pat) == 0 {
		return len(str) == 0
	}

	for i < len(str) {
		if j < len(pat) {
			if pat[j] == '?' || (pat[j] == str[i] && str[i] != '*') {
				i++
				j++
				continue
			}
			if pat[j] == '*' {
				star = j
				pms = i
				j++
				continue
			}
		}
		if star >= 0 {
			pms++
			i = pms
			j = star + 1
			continue
		}
		return false
	}

	for j < len(pat) && pat[j] == '*' {
		j++
	}

	return j == len(pat)
}
