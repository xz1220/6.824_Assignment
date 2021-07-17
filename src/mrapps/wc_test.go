package main

import (
	"fmt"
	"strings"
	"testing"
	"unicode"
)

func ff(r rune) bool {
	return !unicode.IsLetter(r)
}

func TestMap(t *testing.T) {
	strings.FieldsFunc("This is a test!", ff)
	if ff('a') {
		panic("error")
	}
}

func TestPac(t *testing.T) {

	var testFunc = func(num1 int64, num2 ...int64) {
		fmt.Println(num1, num2)
		for _, item := range num2 {
			fmt.Println(item)
		}
	}

	testFunc(1, 2, []int64{2,3,4,5})

}
