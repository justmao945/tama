package intf

import (
	"testing"
)

func TestToArray(t *testing.T) {
	intArray := []int{1, 2, 3}
	ret := ToArray(intArray)
	for i, v := range intArray {
		if v != ret[i].(int) {
			t.Error(v, "!=", ret[i])
		}
	}
}

func TestContains(t *testing.T) {
	intArray := []int{1, 2, 3}
	if !Contains(intArray, 1) {
		t.Error(intArray, "doesn't contain", 1)
	}
	if Contains(intArray, "foo") {
		t.Error(intArray, "contains foo")
	}
}
