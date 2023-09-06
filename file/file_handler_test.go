package file

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"kv/utils"
	"testing"
)

func TestFileInMemory_NewFileInMemoryReader(t *testing.T) {
	fim, err := newFileInMemory("./00001.wal", utils.DefaultOpenFileFlag)
	if err != nil {
		fmt.Printf("newFileInMemory errors: %s", err)
	}

	assert.Equal(t, fim.DataSize, 0)
}

func TestFileInMemory_Append(t *testing.T) {
	fim, err := newFileInMemory("./00001.wal", utils.DefaultOpenFileFlag)
	if err != nil {
		fmt.Printf("newFileInMemory errors: %s", err)
	}
	for i := 0; i < 10; i++ {
		str := fmt.Sprintf("hello %d\n", i)
		bytes := []byte(str)
		err := fim.Append(bytes)
		assert.Equal(t, err, nil)
	}

}

func TestFileInMemory_Sync(t *testing.T) {
	fim, err := newFileInMemory("./00001.wal", utils.DefaultOpenFileFlag)
	if err != nil {
		fmt.Printf("newFileInMemory errors: %s", err)
	}
	fim.Sync()
}

func FIleInMemoryInit() *fileInMemory {
	fim, err := newFileInMemory("./00001.wal", utils.DefaultOpenFileFlag)
	if err != nil {
		fmt.Printf("newFileInMemory errors: %s", err)
	}
	for i := 0; i < 10; i++ {
		str := fmt.Sprintf("hello %d\n", i)
		bytes := []byte(str)
		err := fim.Append(bytes)
		if err != nil {
			return nil
		}
	}
	return fim
}

func TestFileInMemory_AllocateSlice(t *testing.T) {
	fim := FIleInMemoryInit()
	str := "\n--------------------------Test Buffer------------------------------------\n"
	size := len(str)
	err := fim.Truncate(100)
	if err != nil {
		panic(err)
	}
	slice, err := fim.AllocateSlice(size, 20000)
	if err != nil {
		panic(err)
	}
	buf := []byte(str)
	assert.Equal(t, copy(slice, buf), size)

	bytes := fim.Slice(20000)
	s := string(bytes)
	fmt.Printf("len: %d str: %s", len(s), s)

	fim.Sync()
}

func TestFileInMemory_Delete(t *testing.T) {
	fim := FIleInMemoryInit()
	fim.Delete()
	assert.Equal(t, fim.DataSize, 0)
}

func TestFileInMemory_AppendBuffer(t *testing.T) {
	fim := FIleInMemoryInit()
	str := "\n-----|fuck you every day|------This is a Test Module-----\n"
	err := fim.AppendBuffer(400, []byte(str))
	assert.Equal(t, err, nil)
	bytes, err := fim.Bytes(len(str), 400)
	assert.Equal(t, err, nil)
	s := string(bytes)
	assert.Equal(t, s, str)
}
