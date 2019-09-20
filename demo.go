package main

import (
	"fmt"
	"hash/crc32"
	"io"
)

func main() {
	check_str := "170141183460469231731687303715884105727"
	//hash.write(check_str)
	//hash.Sum()
	ieee := crc32.NewIEEE()
	io.WriteString(ieee, check_str)
	s := ieee.Sum32()
	fmt.Printf("IEEE(%s) = 0x%x", check_str, s)
}
