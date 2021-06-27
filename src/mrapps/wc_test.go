package main

import (
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
