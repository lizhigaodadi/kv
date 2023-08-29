package utils

func CondPanic(condition bool, e error) {
	if condition {
		panic(e)
	}
}
