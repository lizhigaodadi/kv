package utils

import "github.com/pkg/errors"

func Copy(b []byte) []byte {
	len := len(b)
	newB := make([]byte, len)
	CondPanic(copy(newB, b) != len, errors.New("copy failed"))

	return newB
}
