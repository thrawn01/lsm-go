package assert

import "fmt"

func True(condition bool, errMsg string) {
	if !condition {
		panic(fmt.Sprintf("Assertion Failed: %s\n", errMsg))
	}
}
