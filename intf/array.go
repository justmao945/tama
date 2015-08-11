// Package intf provides utilities for interface.
package intf

import "reflect"

// ToArray converts `interface{}` to `[]interface{}`.
func ToArray(args interface{}) (ret []interface{}) {
	val := reflect.ValueOf(args)
	if val.Kind() == reflect.Array || val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			ret = append(ret, val.Index(i).Interface())
		}
	}
	return
}

// Contains tests whether `elem` is in `array` or not.
func Contains(array, elem interface{}) bool {
	iarray := ToArray(array)
	if iarray == nil {
		return false
	}
	for _, item := range iarray {
		if item == elem {
			return true
		}
	}
	return false
}
