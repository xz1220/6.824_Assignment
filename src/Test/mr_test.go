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

	TestFile := "test.txt"
	file, err := os.Create(pwd + "/" + TestFile)
	if err != nil {
		log.Printf("file create err")
	}
	defer file.Close()

	// file.Write([]byte("2222"))

	tt := &test{"1"}
	encode := json.NewEncoder(file)
	encode.Encode(tt)

}
