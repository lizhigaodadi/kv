package utils

func CondPanic(condition bool, e error) {
	if condition {
		panic(e)
	}
}

func Panic(e error) {
	if e != nil {
		panic(e)
	}
}
