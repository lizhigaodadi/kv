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

func TestFileInMemory_AppendBuffer(t *testing.T) {
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
