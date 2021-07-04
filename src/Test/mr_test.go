/*
 * @Author: your name
 * @Date: 2021-07-04 23:50:45
 * @LastEditTime: 2021-07-05 00:48:42
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /6.824_Assignment/src/Test/mr_test.go
 */
package Test

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestOSPackage(t *testing.T) {

	// type test struct {
	// 	Name string `json:"name"`
	// }

	pwd, err := os.Getwd()
	if err != nil {
		log.Printf("os Getwd Err")
		panic(err)
	}
	fmt.Println("pwd is - ", pwd)

	TestFile := pwd + "/" + "test.txt"
	var file *os.File
	if checkFileIsExist(TestFile) {
		file, err = os.OpenFile(TestFile, os.O_APPEND, 0666)
		if err != nil {
			log.Printf("file create err")
		}
		// defer file.Close()
	} else {
		file, err = os.Create(TestFile)
		if err != nil {
			log.Printf("file create err")
		}
		// defer file.Close()
	}

	// file.Write([]byte("2222"))

	// tt := &test{"1"}
	// tt2 := &test{"2"}
	// tt3 := &test{"3"}
	// encode := json.NewEncoder(file)
	// encode.Encode(tt)
	// encode.Encode(tt2)
	// encode.Encode(tt2)
	// encode.Encode(tt3)
	// fmt.Fprintf(file, "%v %v\n", "1", "1")
	file.WriteString("ttt")
	file.Write([]byte("ttttttttt"))
	file.Close()
}

/*
 Utils contains some useful methonds.
*/

func checkFileIsExist(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}
