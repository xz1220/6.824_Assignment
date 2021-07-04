package Test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
)

func TestOSPackage(t *testing.T) {

	type test struct {
		Name string `json:"name"`
	}

	pwd, err := os.Getwd()
	if err != nil {
		log.Printf("os Getwd Err")
	}
	fmt.Println("pwd is - ", pwd)

	TestFile := "test.json"
	file, err := os.Open(pwd + "/" + TestFile)
	if err != nil {
		log.Printf("file create err")
	}
	defer file.Close()

	// file.Write([]byte("2222"))

	tt := &test{"1"}
	tt2 := &test{"2"}
	tt3 := &test{"3"}
	encode := json.NewEncoder(file)
	encode.Encode(tt)
	encode.Encode(tt2)
	encode.Encode(tt2)
	encode.Encode(tt3)

}
