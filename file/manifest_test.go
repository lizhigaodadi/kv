package file

import (
	"github.com/stretchr/testify/assert"
	"kv/pb"
	"kv/utils"
	"math/rand"
	"testing"
	"time"
)

const (
	MaxSz = 1 << 15
)

func TestOpenManifestFile(t *testing.T) {
	opt := &Options{
		FID:      1,
		FileName: utils.ManifestFileName,
		Dir:      "./",
		Path:     "./",
		Flag:     utils.DefaultOpenFileFlag,
		MaxSz:    MaxSz,
	}
	file, err := OpenManifestFile(opt)
	assert.Equal(t, err, nil)
	assert.Equal(t, file.mf.Creation, 0)
}

func TestManifest_ApplyChangeSet(t *testing.T) {
	opt := &Options{
		FID:      1,
		FileName: utils.ManifestFileName,
		Dir:      "./",
		Path:     "./",
		Flag:     utils.DefaultOpenFileFlag,
		MaxSz:    MaxSz,
	}
	file, err := OpenManifestFile(opt)
	assert.Equal(t, err, nil)
	/*Test ApplyChanges*/
	rand.Seed(time.Now().UnixNano())
	changes := make([]*pb.ManifestChange, 0)
	for i := 100; i < 200; i++ {
		mc := &pb.ManifestChange{
			Id:       uint64(i),
			Op:       pb.Operation_CREATE,
			Level:    uint32(rand.Intn(7)),
			Checksum: utils.U32ToBytes(uint32(i)),
		}
		changes = append(changes, mc)
	}
	err = file.AddChanges(changes)
	assert.Equal(t, err, nil)
}

func TestManifest_ApplyChange_Concurrency(t *testing.T) {
	/*TODO:测试并发环境下的写入文件*/
}
