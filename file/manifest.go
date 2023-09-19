package file

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"kv/pb"
	"kv/utils"
	"kv/utils/cache"
	"os"
	"path/filepath"
	"sync"
)

type LevelManager struct {
	maxFd        uint64
	opt          *Options
	cache        *cache.Cache
	manifestFile *ManifestFile
}
type ManifestFile struct {
	opt                       *Options
	f                         *os.File
	lock                      sync.RWMutex
	deletionsRewriteThreshold int
	mf                        *Manifest
}

type levelManifest struct {
	Tables map[uint64]struct{}
}

type TableManifest struct {
	Level    uint8
	CheckSum []byte
}
type levelHandler struct {
	rw       sync.RWMutex
	levelNum int
	/*TODO:　 table*/
}

type Manifest struct {
	level    []levelManifest
	Tables   map[uint64]TableManifest
	Creation int
	Deletion int
}

func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	/*TODO:打开/创建manifest文件*/

	/*获取文件路径和文件名称*/
	fileName := filepath.Join(opt.Dir, utils.ManifestFileName)
	/*打开文件而不创建文件通过这种方式判断是否是第一次创建*/
	fp, err := os.OpenFile(fileName, os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			/*TODO:创建新的manifest文件*/
			manifest, err := createManifest()
			if err != nil {
				return nil, err
			}
			/*创建一个空的manifest文件*/
			fp, netCreation, err := helpRewrite(opt.Dir, manifest)

			mf := &ManifestFile{
				f:                         fp,
				opt:                       opt,
				lock:                      sync.RWMutex{},
				deletionsRewriteThreshold: utils.DefaultDeletionsRewriteThreshold,
				mf:                        manifest,
			}
			mf.mf.Creation += netCreation

			return mf, nil

		} else {
			return nil, err
		}
	}
	mf, err := createManifest()
	if err != nil {
		return nil, err
	}
	manifestFile := &ManifestFile{
		opt:                       opt,
		f:                         fp,
		lock:                      sync.RWMutex{},
		deletionsRewriteThreshold: utils.DefaultDeletionsRewriteThreshold,
		mf:                        mf,
	}
	/*TODO:从磁盘中加载我们的manifest文件信息出来*/
	if err = manifestFile.loadManifest(); err != nil {
		return nil, err
	}

	return manifestFile, nil

}

func createManifest() (*Manifest, error) {
	/*创建一个新的manifest*/
	level := make([]levelManifest, 0)
	return &Manifest{
		level:  level,
		Tables: make(map[uint64]TableManifest),
	}, nil
}

func helpRewrite(dir string, manifest *Manifest) (*os.File, int, error) {
	/*TODO:读取manifest中的文件信息来进行重写文件*/
	rewriteFileName := filepath.Join(dir, utils.ManifestRewriteName)

	/*创建新文件*/
	fp, err := os.OpenFile(rewriteFileName, utils.DefaultOpenFileFlag, utils.DefaultFileAclMask)
	if err != nil {
		fmt.Printf("ErrorMessage : %s", err)
		return nil, 0, err
	}
	/*读取原来的Manifest文件信息*/
	buf := make([]byte, 8) /*设置相关的信息*/
	binary.BigEndian.PutUint32(buf[0:4], utils.MagicText)
	binary.BigEndian.PutUint32(buf[4:8], utils.MagicVersion)

	/*将其序列化加入到文件中*/
	netCreations := len(manifest.Tables)
	if netCreations != 0 { /*根本不需要继续添加了*/
		manifestChangeSet := manifest.asManifestChangeSet()
		manifestBuf, err := proto.Marshal(manifestChangeSet)
		if err != nil {
			return nil, 0, err
		}
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(manifestBuf)))
		/*计算校验和*/
		checksum := utils.CalculateChecksum(manifestBuf)
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], uint32(checksum))
		/*之前的manifest buffer 块加入到新的buffer中*/
		buf = append(buf, lenCrcBuf[:]...) /*因为lenCrcBuf是数组，我们必须先将其转化为切片*/
		buf = append(buf, manifestBuf...)
	}

	/*将数据写入刷新到磁盘中*/
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	/**/
	if err = fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}
	/*关闭文件并对其重命名*/
	if err = fp.Close(); err != nil {
		return nil, 0, err
	}
	manifestName := filepath.Join(dir, utils.ManifestFileName)
	if err = os.Rename(rewriteFileName, manifestName); err != nil {
		return nil, 0, err
	}
	/*重新打开文件返回*/
	if fp, err = os.OpenFile(manifestName, utils.DefaultOpenFileFlag, utils.DefaultFileAclMask); err != nil {
		return nil, 0, err
	}

	/*将文件读取指针移动到末尾*/
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}

	if err = utils.SyncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, netCreations, nil

}

func (m *Manifest) asManifestChangeSet() *pb.ManifestChangeSet {
	manifestChanges := make([]*pb.ManifestChange, 0)
	for fid, item := range m.Tables {
		manifestChanges = append(manifestChanges, newManifestChange(fid, uint32(item.Level), item.CheckSum))
	}
	return &pb.ManifestChangeSet{
		Changes: manifestChanges,
	}
}

func newManifestChange(id uint64, level uint32, checkSum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Level:    level,
		Op:       pb.Operation_CREATE,
		Checksum: checkSum,
	}
}

type manifestReader struct {
	bufReader *bufio.Reader
	count     int
}

func (mr *manifestReader) Read(p []byte) (int, error) {
	return mr.bufReader.Read(p)
}

func (mf *ManifestFile) loadManifest() error {
	/*从磁盘中读取我们所需要的manifest文件*/
	fp := mf.f        /*该文件必然是已经打开了的*/
	var offset uint32 /*记录我们一共读取了多少数据，最后进行一次切割，将多余的数据清除*/
	reader := &manifestReader{bufReader: bufio.NewReader(fp)}
	var magicBuf [8]byte
	n, err := io.ReadFull(reader, magicBuf[:])
	utils.CondPanic(n != utils.MagicLen, errors.New("Invalid magic Length"))
	if err != nil {
		return err
	}
	/*开始校验信息*/
	magicText := binary.BigEndian.Uint32(magicBuf[0:4])
	magicVersion := binary.BigEndian.Uint32(magicBuf[4:8])
	utils.CondPanic(magicText != utils.MagicText || magicVersion != utils.MagicVersion,
		errors.New("Invalid magic"))
	offset += 8
	for {
		var crcLenBuf [8]byte
		if _, err = io.ReadFull(reader, crcLenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break /*结束了*/
			}
			return err
		}
		bufLen := binary.BigEndian.Uint32(crcLenBuf[0:4])
		if bufLen == 0 {
			break /*不需要再继续了*/
		}
		offset += 8
		buf := make([]byte, bufLen)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil
		}
		/*检验校验和*/
		if err := utils.VerifyCheckSum(buf, crcLenBuf[4:8]); err != nil {
			return errors.New("verify crc failed")
		}
		/*解析数据*/
		changeSet := &pb.ManifestChangeSet{}
		if err := proto.Unmarshal(buf, changeSet); err != nil {
			return err
		}
		err := mf.AddChanges(changeSet.Changes)
		if err != nil {
			return err
		}
		/*计算changeSet的长度*/

		offset += bufLen
	}

	/*在此处进行一次切割*/
	if err = mf.f.Truncate(int64(offset)); err != nil {
		mf.f.Close()
		return err
	}
	if _, err = mf.f.Seek(0, io.SeekEnd); err != nil {
		mf.f.Close()
		return err
	}
	return nil
}

func (mf *Manifest) ApplyChange(change *pb.ManifestChange) error {
	return mf.applyChange(change)
}
func (mf *Manifest) applyChange(change *pb.ManifestChange) error {
	switch change.GetOp() {
	case pb.Operation_CREATE:
		{ /*判断一下是否已经存在*/
			if _, err := mf.Tables[change.Id]; err {
				return fmt.Errorf("create mafiest change Id: %d has existed", change.Id)
			}
			mf.Tables[change.Id] = TableManifest{
				Level:    uint8(change.GetLevel()),
				CheckSum: change.Checksum,
			}
			/*判断一下Levels是否超出高度*/
			for len(mf.level) <= int(change.Level) {
				mf.level = append(mf.level, levelManifest{Tables: make(map[uint64]struct{})})
			}
			mf.level[change.Level].Tables[change.Id] = struct{}{}
			mf.Creation++
		}
	case pb.Operation_DELETE:
		{
			if _, err := mf.Tables[change.Id]; !err {
				/*删除一个不存在的目标操作有问题*/
				return fmt.Errorf("delete change Id: %d not existed", change.Id)
			}
			delete(mf.Tables, change.Id)
			delete(mf.level[change.Id].Tables, change.Id)
			mf.Deletion--
		}

	}

	return nil
}

func (m *Manifest) ApplyChangeSet(changeSet *pb.ManifestChangeSet) error {
	/*解析changeSet，加入到Manifest中*/

	return m.applyChangeSet(changeSet)
}

func (m *Manifest) applyChangeSet(changeSet *pb.ManifestChangeSet) error {
	/*解析change，加入到Manifest中*/
	for _, item := range changeSet.Changes {
		if err := m.applyChange(item); err != nil {
			return err
		}
	}
	return nil
}

func (mf *ManifestFile) AddChanges(changes []*pb.ManifestChange) error {
	/*同时对Manifest磁盘和内存的部分都添加上新的信息*/
	changeSet := &pb.ManifestChangeSet{Changes: changes}
	buf, err := proto.Marshal(changeSet)
	if err != nil {
		return err
	}
	bufLen := len(buf)

	/*将信息添加到内存的结构中*/
	mf.lock.Lock()
	defer mf.lock.Unlock()
	if err = mf.mf.ApplyChangeSet(changeSet); err != nil {
		return err
	}

	/*判断一下是否超出了阈值*/
	if mf.mf.Deletion > utils.ManifestDeletionsRewriteThreshold &&
		mf.mf.Deletion > utils.ManifestDeletionRatio*(mf.mf.Creation-mf.mf.Deletion) {
		if err := mf.reWrite(); err != nil {
			return err
		}

	} else {
		/*直接加入到磁盘中*/
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(bufLen))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, utils.CastageoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err = mf.f.Write(buf); err != nil { /*f的文件指针必须一直保持在最后*/
			return err
		}
	}

	/*刷新文件系统*/
	err = mf.f.Sync()
	return err
}

func (mf *ManifestFile) Close() error {
	return mf.f.Close()
}

/*检查是否有多余的Manifest信息或者缺少了某些Manifest信息*/
func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	for id := range mf.mf.Tables {
		if _, ok := idMap[id]; !ok {
			/*发现了Table中的manifest信息,idMap中不存在*/
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	for id := range idMap {
		if _, ok := mf.mf.Tables[id]; !ok {
			/*发现错误*/
			_ = utils.Err(fmt.Errorf("tables not exists id: %d", id))
			/*获取到相关文件信息*/
			fileName := utils.FileNameSSTable(mf.opt.Dir, id)
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	return nil
}

/*执行该方法的时候必须加锁,该方法就是将所有的deletion操作删除掉，全部变成creation操作*/
func (mf *ManifestFile) reWrite() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	fp, creation, err := helpRewrite(mf.opt.Dir, mf.mf)
	if err != nil {
		return err
	}

	mf.f = fp
	mf.mf.Creation = creation
	mf.mf.Deletion = 0
	return nil
}

type TableMeta struct {
	Id       uint64
	CheckSum []byte
}

func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) error {
	return mf.AddChanges([]*pb.ManifestChange{
		newManifestChange(t.Id, uint32(levelNum), t.CheckSum),
	})
}

func (mf *ManifestFile) ManiFest() *Manifest {
	return mf.mf
}
