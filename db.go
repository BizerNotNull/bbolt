package bbolt

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
	"unsafe"

	berrors "go.etcd.io/bbolt/errors"
	"go.etcd.io/bbolt/internal/common"
	fl "go.etcd.io/bbolt/internal/freelist"
)

// 连续文件锁定尝试之间的时间间隔。
const flockRetryTimeout = 50 * time.Millisecond

// FreelistType 是 freelist 后端的类型
type FreelistType string

// TODO(ahrtr): 最终我们应该（逐步）
//  1. 默认使用 `FreelistMapType`;
//  2. 删除 `FreelistArrayType`，不导出 `FreelistMapType`
//     并从 `DB` 和 `Options` 中移除字段 `FreelistType`；
const (
	// FreelistArrayType 表示后端 freelist 类型为数组
	FreelistArrayType = FreelistType("array")
	// FreelistMapType 表示后端 freelist 类型为哈希表
	FreelistMapType = FreelistType("hashmap")
)

// DB 表示持久化到磁盘文件的一组 bucket。
// 所有数据访问都通过事务（transaction）进行，可通过 DB 获取。
// 在调用 Open() 之前访问 DB 的所有函数都会返回 ErrDatabaseNotOpen。
type DB struct {
	// 启用时，数据库将在每次提交后执行 Check()。
	// 如果数据库处于不一致状态将触发 panic。此标志对性能影响较大，因而仅用于调试。
	StrictMode bool

	// 设置 NoSync 标志将导致数据库在每次提交后跳过 fsync() 调用。
	// 在批量加载数据时可能有用，允许在系统故障或数据库损坏时重新启动批量加载。
	// 正常使用时不要设置此标志。
	//
	// 如果包级常量 IgnoreNoSync 为 true，则此值会被忽略。详见该常量的注释。
	//
	// 这是不安全的。请谨慎使用。
	NoSync bool

	// 为 true 时，跳过将 freelist 同步到磁盘。这在正常操作下提高写性能，
	// 但在恢复期间需要完整的数据库重新同步。
	NoFreelistSync bool

	// FreelistType 设置后端 freelist 类型。有两种选择。Array（数组）简单但当数据库很大且 freelist 分散时会出现显著性能下降。
	// 另一种是使用 hashmap，几乎在所有情况下都更快，但不能保证返回最小的可用页 id。通常是安全的。
	// 默认类型为 array
	FreelistType FreelistType

	// 为 true 时，增长数据库时跳过 truncate 调用。
	// 将此设置为 true 仅在非 ext3/ext4 系统上安全。
	// 跳过截断可以避免硬盘空间的预分配，并绕过 remapping 时的 truncate() 和 fsync() 系统调用。
	//
	// https://github.com/boltdb/bolt/issues/284
	NoGrowSync bool

	// 当 `true` 时，bbolt 打开 DB 时会始终加载空闲页（free pages）。
	// 在以写模式打开数据库时，此标志将自动设置为 `true`。
	PreLoadFreelist bool

	// 如果想快速读取整个数据库，可以在 Linux 2.6.23+ 上将 MmapFlag 设置为 syscall.MAP_POPULATE，以便顺序读取预读。
	MmapFlags int

	// MaxBatchSize 是批处理的最大大小。默认值在 Open 中从 DefaultMaxBatchSize 复制。
	//
	// 如果 <=0，则禁用批处理。
	//
	// 不要在调用 Batch 时并发修改此值。
	MaxBatchSize int

	// MaxBatchDelay 是在批处理开始前的最大延迟。
	// 默认值在 Open 中从 DefaultMaxBatchDelay 复制。
	//
	// 如果 <=0，则等同于禁用批处理。
	//
	// 不要在调用 Batch 时并发修改此值。
	MaxBatchDelay time.Duration

	// AllocSize 是在数据库需要创建新页面时分配的空间量。这用来摊薄 truncate() 和 fsync() 的开销，以便增长数据文件。
	AllocSize int

	// MaxSize 是数据文件允许的最大大小（字节）。
	// 如果调用方尝试添加数据导致需要增长数据文件，则会返回错误，且不会增长数据文件。
	// <=0 表示无限制。
	MaxSize int

	// Mlock 在设置为 true 时将数据库文件锁定到内存中。
	// 它可以防止发生大量主页面错误（major page faults），但被使用的内存不能被回收。
	//
	// 仅在 Unix 系统上通过 mlock/munlock 系统调用支持。
	Mlock bool

	logger Logger

	path     string
	openFile func(string, int, os.FileMode) (*os.File, error)
	file     *os.File
	// `dataref` 在 Windows 上根本不会使用，而 golangci-lint 在 Windows 平台上总是失败。
	//nolint
	dataref  []byte // 只读 mmap，写入会触发 SEGV
	data     *[common.MaxMapSize]byte
	datasz   int
	meta0    *common.Meta
	meta1    *common.Meta
	pageSize int
	opened   bool
	rwtx     *Tx
	stats    *Stats

	freelist     fl.Interface
	freelistLoad sync.Once

	pagePool sync.Pool

	batchMu sync.Mutex
	batch   *batch

	rwlock   sync.Mutex   // 只允许一个写者同时存在。
	metalock sync.Mutex   // 保护 meta 页面访问。
	mmaplock sync.RWMutex // remapping 期间保护 mmap 访问。
	statlock sync.RWMutex // 保护统计信息访问。

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// 只读模式。
	// 为 true 时，Update() 和 Begin(true) 会立即返回 ErrDatabaseReadOnly。
	readOnly bool
}

// Path 返回当前打开数据库文件的路径。
func (db *DB) Path() string {
	return db.path
}

// GoString 返回数据库的 Go 字符串表示。
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

// String 返回数据库的字符串表示。
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open 在给定路径以给定文件模式创建并打开数据库。
// 如果文件不存在则会以给定文件模式自动创建。
// 传入 nil options 将导致 Bolt 使用默认选项打开数据库。
// 注意：对于读写事务，确保拥有者对创建/打开的数据库文件具有写权限，例如 0600
func Open(path string, mode os.FileMode, options *Options) (db *DB, err error) {
	db = &DB{
		opened: true,
	}

	// 如果没有提供选项则设置默认选项。
	if options == nil {
		options = DefaultOptions
	}
	db.NoSync = options.NoSync
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags
	db.NoFreelistSync = options.NoFreelistSync
	db.PreLoadFreelist = options.PreLoadFreelist
	db.FreelistType = options.FreelistType
	db.Mlock = options.Mlock
	db.MaxSize = options.MaxSize

	// 后续 DB 操作的默认值。
	db.MaxBatchSize = common.DefaultMaxBatchSize
	db.MaxBatchDelay = common.DefaultMaxBatchDelay
	db.AllocSize = common.DefaultAllocSize

	if !options.NoStatistics {
		db.stats = new(Stats)
	}

	if options.Logger == nil {
		db.logger = getDiscardLogger()
	} else {
		db.logger = options.Logger
	}

	lg := db.Logger()
	if lg != discardLogger {
		lg.Infof("Opening db file (%s) with mode %s and with options: %s", path, mode, options)
		defer func() {
			if err != nil {
				lg.Errorf("Opening bbolt db (%s) failed: %v", path, err)
			} else {
				lg.Infof("Opening bbolt db (%s) successfully", path)
			}
		}()
	}

	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	} else {
		// 写模式下总是加载空闲页
		db.PreLoadFreelist = true
		flag |= os.O_CREATE
	}

	db.openFile = options.OpenFile
	if db.openFile == nil {
		db.openFile = os.OpenFile
	}

	// 打开数据文件并为元数据写入分离 sync 处理器。
	if db.file, err = db.openFile(path, flag, mode); err != nil {
		_ = db.close()
		lg.Errorf("failed to open db file (%s): %v", path, err)
		return nil, err
	}
	db.path = db.file.Name()

	// 锁定文件，以便使用 Bolt 的其他进程在读写模式下不能同时使用数据库。
	// 否则会导致损坏，因为两个进程会分别写入 meta 页面和空闲页面。
	// 如果 !options.ReadOnly，则以独占锁定（仅一个进程可以获取锁）。
	// 否则（options.ReadOnly 设置时）使用共享锁（允许多个进程同时持有锁）。
	if err = flock(db, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		lg.Errorf("failed to lock db file (%s), readonly: %t, error: %v", path, db.readOnly, err)
		return nil, err
	}

	// 测试钩子的默认值
	db.ops.writeAt = db.file.WriteAt

	if db.pageSize = options.PageSize; db.pageSize == 0 {
		// 将默认页面大小设置为操作系统页面大小。
		db.pageSize = common.DefaultPageSize
	}

	// 如果文件不存在则初始化数据库。
	if info, statErr := db.file.Stat(); statErr != nil {
		_ = db.close()
		lg.Errorf("failed to get db file's stats (%s): %v", path, err)
		return nil, statErr
	} else if info.Size() == 0 {
		// 使用元页面初始化新文件。
		if err = db.init(); err != nil {
			// 初始化失败时清理文件描述符
			_ = db.close()
			lg.Errorf("failed to initialize db file (%s): %v", path, err)
			return nil, err
		}
	} else {
		// 尝试从元数据页面获取页面大小
		if db.pageSize, err = db.getPageSize(); err != nil {
			_ = db.close()
			lg.Errorf("failed to get page size from db file (%s): %v", path, err)
			return nil, err
		}
	}

	// 初始化页面池。
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// 将数据文件映射到内存。
	if err = db.mmap(options.InitialMmapSize); 
	err != nil {
		_ = db.close()
		lg.Errorf("failed to map db file (%s): %v", path, err)
		return nil, err
	}

	if db.PreLoadFreelist {
		db.loadFreelist()
	}

	if db.readOnly {
		return db, nil
	}

	// 在从无同步转换到有同步时刷新 freelist，以便不了解 NoFreelistSync 的 boltdb 可以之后打开 db。
	if !db.NoFreelistSync && !db.hasSyncedFreelist() {
		tx, txErr := db.Begin(true)
		if tx != nil {
			txErr = tx.Commit()
		}
		if txErr != nil {
			lg.Errorf("starting readwrite transaction failed: %v", txErr)
			_ = db.close()
			return nil, txErr
		}
	}

	// 标记数据库已打开并返回。
	return db, nil
}

// getPageSize 从 meta 页面读取 pageSize。它首先尝试读取第一个 meta 页面。
// 如果第一个页面无效，则尝试使用默认页面大小读取第二个页面。
func (db *DB) getPageSize() (int, error) {
	var (
		meta0CanRead, meta1CanRead bool
	)

	// 读取第一个 meta 页面以确定页面大小。
	if pgSize, canRead, err := db.getPageSizeFromFirstMeta(); err != nil {
		// 我们无法从第 0 页读取页面大小，但可以读取第 0 页。
		meta0CanRead = canRead
	} else {
		return pgSize, nil
	}

	// 读取第二个 meta 页面以确定页面大小。
	if pgSize, canRead, err := db.getPageSizeFromSecondMeta(); err != nil {
		// 我们无法从第 1 页读取页面大小，但可以读取第 1 页。
		meta1CanRead = canRead
	} else {
		return pgSize, nil
	}

	// 如果我们无法从两个页面读取页面大小，但能读取任一页面，则假设它与操作系统相同或与给定值相同，
	// 因为这就是页面大小最初被选择的方式。
	//
	// 如果两个页面都无效，并且（该操作系统使用与创建数据库时不同的页面大小或给定页面大小与创建时不同），
	// 那么我们就没办法访问数据库。
	if meta0CanRead || meta1CanRead {
		return db.pageSize, nil
	}

	return 0, berrors.ErrInvalid
}

// getPageSizeFromFirstMeta 从第一个 meta 页面读取 pageSize
func (db *DB) getPageSizeFromFirstMeta() (int, bool, error) {
	var buf [0x1000]byte
	var metaCanRead bool
	if bw, err := db.file.ReadAt(buf[:], 0); err == nil && bw == len(buf) {
		metaCanRead = true
		if m := db.pageInBuffer(buf[:], 0).Meta(); m.Validate() == nil {
			return int(m.PageSize()), metaCanRead, nil
		}
	}
	return 0, metaCanRead, berrors.ErrInvalid
}

// getPageSizeFromSecondMeta 从第二个 meta 页面读取 pageSize
func (db *DB) getPageSizeFromSecondMeta() (int, bool, error) {
	var (
		fileSize    int64
		metaCanRead bool
	)

	// 获取 db 文件大小
	if info, err := db.file.Stat(); err != nil {
		return 0, metaCanRead, err
	} else {
		fileSize = info.Size()
	}

	// 我们需要读取第二个 meta 页面，所以必须跳过第一个页面；
	// 但我们还不知道确切的页面大小，这是一个鸡生蛋问题。
	// 解决方法是尝试所有可能的页面大小，从 1KB 开始直到 16MB (1024<<14) 或 db 文件末尾
	//
	// TODO: 是否应该支持更大的页面大小？
	for i := 0; i <= 14; i++ {
		var buf [0x1000]byte
		var pos int64 = 1024 << uint(i)
		if pos >= fileSize-1024 {
			break
		}
		bw, err := db.file.ReadAt(buf[:], pos)
		if (err == nil && bw == len(buf)) || (err == io.EOF && int64(bw) == (fileSize-pos)) {
			metaCanRead = true
			if m := db.pageInBuffer(buf[:], 0).Meta(); m.Validate() == nil {
				return int(m.PageSize()), metaCanRead, nil
			}
		}
	}

	return 0, metaCanRead, berrors.ErrInvalid
}

// loadFreelist 如果 freelist 已被同步则读取 freelist，否则通过扫描 DB 重建它。
// 假定不会有并发访问 freelist。
func (db *DB) loadFreelist() {
	db.freelistLoad.Do(func() {
		db.freelist = newFreelist(db.FreelistType)
		if !db.hasSyncedFreelist() {
			// 通过扫描 DB 重建 freelist。
			db.freelist.Init(db.freepages())
		} else {
			// 从 freelist 页面读取 freelist。
			db.freelist.Read(db.page(db.meta().Freelist()))
		}
		if db.stats != nil {
			db.stats.FreePageN = db.freelist.FreeCount()
		}
	})
}

func (db *DB) hasSyncedFreelist() bool {
	return db.meta().Freelist() != common.PgidNoFreelist
}

func (db *DB) fileSize() (int, error) {
	info, err := db.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("file stat error: %w", err)
	}
	sz := int(info.Size())
	if sz < db.pageSize*2 {
		return 0, fmt.Errorf("file size too small %d", sz)
	}
	return sz, nil
}

// mmap 打开底层内存映射文件并初始化 meta 引用。
// minsz 是新 mmap 的最小大小。
func (db *DB) mmap(minsz int) (err error) {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	lg := db.Logger()

	// 确保大小至少为最小值。
	var fileSize int
	fileSize, err = db.fileSize()
	if err != nil {
		lg.Errorf("getting file size failed: %w", err)
		return err
	}
	var size = fileSize
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		lg.Errorf("getting map size failed: %w", err)
		return err
	}

	if db.Mlock {
		// 解锁 db 内存
		if err := db.munlock(fileSize); err != nil {
			return err
		}
	}

	// 在解除映射之前取消对所有 mmap 引用的引用。
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// 在继续之前解除已有的映射。
	if err = db.munmap(); err != nil {
		return err
	}

	// 将数据文件映射为字节切片。
	// gofail: var mapError string
	// return errors.New(mapError)
	if err = mmap(db, size); err != nil {
		lg.Errorf("[GOOS: %s, GOARCH: %s] mmap failed, size: %d, error: %v", runtime.GOOS, runtime.GOARCH, size, err)
		return err
	}

	// 在任何错误时执行 unmmap 以重置所有数据字段：
	// dataref, data, datasz, meta0 和 meta1。
	defer func() {
		if err != nil {
			if unmapErr := db.munmap(); unmapErr != nil {
				err = fmt.Errorf("%w; rollback unmap also failed: %v", err, unmapErr)
			}
		}
	}()

	if db.Mlock {
		// 不允许数据文件被换出
		if err := db.mlock(fileSize); err != nil {
			return err
		}
	}

	// 保存 meta 页面引用。
	db.meta0 = db.page(0).Meta()
	db.meta1 = db.page(1).Meta()

	// 验证 meta 页面。只有在两个 meta 页面都验证失败时才返回错误，
	// 因为 meta0 验证失败意味着它没有正确保存 -- 但可以使用 meta1 恢复。反之亦然。
	err0 := db.meta0.Validate()
	err1 := db.meta1.Validate()
	if err0 != nil && err1 != nil {
		lg.Errorf("both meta pages are invalid, meta0: %v, meta1: %v", err0, err1)
		return err0
	}

	return nil
}

func (db *DB) invalidate() {
	db.dataref = nil
	db.data = nil
	db.datasz = 0

	db.meta0 = nil
	db.meta1 = nil
}

// munmap 解除数据文件的内存映射。
func (db *DB) munmap() error {
	defer db.invalidate()

	// gofail: var unmapError string
	// return errors.New(unmapError)
	if err := munmap(db); err != nil {
		db.Logger().Errorf("[GOOS: %s, GOARCH: %s] munmap failed, db.datasz: %d, error: %v", runtime.GOOS, runtime.GOARCH, db.datasz, err)
		return fmt.Errorf("unmap error: %w", err)
	}

	return nil
}

// mmapSize 根据数据库当前大小确定 mmap 的合适大小。
// 最小为 32KB，并逐步翻倍直到 1GB。若新 mmap 大于允许的最大值则返回错误。
func (db *DB) mmapSize(size int) (int, error) {
	// 从 32KB 开始翻倍直到 1GB。
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// 验证请求的大小不超过最大允许值。
	if size > common.MaxMapSize {
		return 0, errors.New("mmap too large")
	}

	// 如果大于 1GB，则每次增长 1GB。
	sz := int64(size)
	if remainder := sz % int64(common.MaxMmapStep); remainder > 0 {
		sz += int64(common.MaxMmapStep) - remainder
	}

	// 确保 mmap 大小是页面大小的整数倍。
	// 这应该总是成立，因为我们以 MB 为单位递增。
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// 如果超过最大值则只增长到最大值。
	if sz > common.MaxMapSize {
		sz = common.MaxMapSize
	}

	return int(sz), nil
}

func (db *DB) munlock(fileSize int) error {
	// gofail: var munlockError string
	// return errors.New(munlockError)
	if err := munlock(db, fileSize); err != nil {
		db.Logger().Errorf("[GOOS: %s, GOARCH: %s] munlock failed, fileSize: %d, db.datasz: %d, error: %v", runtime.GOOS, runtime.GOARCH, fileSize, db.datasz, err)
		return fmt.Errorf("munlock error: %w", err)
	}
	return nil
}

func (db *DB) mlock(fileSize int) error {
	// gofail: var mlockError string
	// return errors.New(mlockError)
	if err := mlock(db, fileSize); err != nil {
		db.Logger().Errorf("[GOOS: %s, GOARCH: %s] mlock failed, fileSize: %d, db.datasz: %d, error: %v", runtime.GOOS, runtime.GOARCH, fileSize, db.datasz, err)
		return fmt.Errorf("mlock error: %w", err)
	}
	return nil
}

func (db *DB) mrelock(fileSizeFrom, fileSizeTo int) error {
	if err := db.munlock(fileSizeFrom); err != nil {
		return err
	}
	if err := db.mlock(fileSizeTo); err != nil {
		return err
	}
	return nil
}

// init 创建一个新的数据库文件并初始化其 meta 页面。
func (db *DB) init() error {
	// 在缓冲区上创建两个 meta 页面。
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf, common.Pgid(i))
		p.SetId(common.Pgid(i))
		p.SetFlags(common.MetaPageFlag)

		// 初始化 meta 页面。
		m := p.Meta()
		m.SetMagic(common.Magic)
		m.SetVersion(common.Version)
		m.SetPageSize(uint32(db.pageSize))
		m.SetFreelist(2)
		m.SetRootBucket(common.NewInBucket(3, 0))
		m.SetPgid(4)
		m.SetTxid(common.Txid(i))
		m.SetChecksum(m.Sum64())
	}

	// 在第 3 页写入一个空的 freelist。
	p := db.pageInBuffer(buf, common.Pgid(2))
	p.SetId(2)
	p.SetFlags(common.FreelistPageFlag)
	p.SetCount(0)

	// 在第 4 页写入一个空的 leaf 页面。
	p = db.pageInBuffer(buf, common.Pgid(3))
	p.SetId(3)
	p.SetFlags(common.LeafPageFlag)
	p.SetCount(0)

	// 将缓冲区写入数据文件。
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		db.Logger().Errorf("writeAt failed: %w", err)
		return err
	}
	if err := fdatasync(db); err != nil {
		db.Logger().Errorf("[GOOS: %s, GOARCH: %s] fdatasync failed: %w", runtime.GOOS, runtime.GOARCH, err)
		return err
	}

	return nil
}

// Close 释放所有数据库资源。
// 它会在关闭数据库并返回之前阻塞等待任何打开的事务完成。
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	db.freelist = nil

	// 清除 ops。
	db.ops.writeAt = nil

	var errs []error
	// 关闭 mmap。
	if err := db.munmap(); err != nil {
		errs = append(errs, err)
	}

	// 关闭文件句柄。
	if db.file != nil {
		// 只读文件无需解锁。
		if !db.readOnly {
			// 解锁文件。
			if err := funlock(db); err != nil {
				errs = append(errs, fmt.Errorf("bolt.Close(): funlock error: %w", err))
			}
		}

		// 关闭文件描述符。
		if err := db.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("db file close: %w", err))
		}
		db.file = nil
	}

	db.path = ""

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Begin 启动一个新的事务。
// 多个只读事务可以并发使用，但同时只能有一个写事务。
// 启动多个写事务将导致调用阻塞并序列化，直到当前写事务结束。
//
// 事务不应彼此依赖。在同一 goroutine 中打开只读事务和写事务可能会导致死锁，
// 因为数据库在增长并需要重新 mmap 时无法在有打开的读事务时进行。
//
// 如果需要长期运行的只读事务（例如快照事务），你可能希望将 DB.InitialMmapSize 设置得足够大以避免写事务被阻塞。
//
// 重要提示：在完成只读事务后必须关闭它们，否则数据库将无法回收旧页面。
func (db *DB) Begin(writable bool) (t *Tx, err error) {
	if lg := db.Logger(); lg != discardLogger {
		lg.Debugf("Starting a new transaction [writable: %t]", writable)
		defer func() {
			if err != nil {
				lg.Errorf("Starting a new transaction [writable: %t] failed: %v", writable, err)
			} else {
				lg.Debugf("Starting a new transaction [writable: %t] successfully", writable)
			}
		}()
	}

	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

func (db *DB) Logger() Logger {
	if db == nil || db.logger == nil {
		return getDiscardLogger()
	}
	return db.logger
}

func (db *DB) beginTx() (*Tx, error) {
	// 在初始化事务时锁定 meta 页面。我们在获取 mmap 锁之前先获取 meta 锁，因为写事务获取锁的顺序也是如此。
	db.metalock.Lock()

	// 对 mmap 获取只读锁。当 mmap 重新映射时会获取写锁，因此在重新映射前所有事务必须完成。
	db.mmaplock.RLock()

	// 若数据库尚未打开则退出。
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, berrors.ErrDatabaseNotOpen
	}

	// 若数据库未正确映射则退出。
	if db.data == nil {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, berrors.ErrInvalidMapping
	}

	// 创建与数据库关联的事务。
	t := &Tx{}
	t.init(db)

	if db.freelist != nil {
		db.freelist.AddReadonlyTXID(t.meta.Txid())
	}

	// 解锁 meta 页面。
	db.metalock.Unlock()

	// 更新事务统计。
	if db.stats != nil {
		db.statlock.Lock()
		db.stats.TxN++
		db.stats.OpenTxN++
		db.statlock.Unlock()
	}

	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	// 如果数据库以 Options.ReadOnly 打开，则返回错误。
	if db.readOnly {
		return nil, berrors.ErrDatabaseReadOnly
	}

	// 获取写者锁。事务关闭时会释放该锁。
	// 这保证了一次只有一个写事务。
	db.rwlock.Lock()

	// 一旦获得写者锁，我们就可以锁定 meta 页面以便设置事务。
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// 若数据库尚未打开则退出。
	if !db.opened {
		db.rwlock.Unlock()
		return nil, berrors.ErrDatabaseNotOpen
	}

	// 若数据库未正确映射则退出。
	if db.data == nil {
		db.rwlock.Unlock()
		return nil, berrors.ErrInvalidMapping
	}

	// 创建与数据库关联的写事务。
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t
	db.freelist.ReleasePendingPages()
	return t, nil
}

// removeTx 从数据库中移除一个事务。
func (db *DB) removeTx(tx *Tx) {
	// 释放对 mmap 的读锁。
	db.mmaplock.RUnlock()

	// 使用 meta 锁来限制对 DB 对象的访问。
	db.metalock.Lock()

	if db.freelist != nil {
		db.freelist.RemoveReadonlyTXID(tx.meta.Txid())
	}

	// 解锁 meta 页面。
	db.metalock.Unlock()

	// 合并统计。
	if db.stats != nil {
		db.statlock.Lock()
		db.stats.OpenTxN--
		db.stats.TxStats.add(&tx.stats)
		db.statlock.Unlock()
	}
}

// Update 在受管理的读写事务上下文中执行一个函数。
// 如果函数返回 nil，则事务提交；如果返回错误，则回滚整个事务。
// 函数或提交返回的任何错误都会由 Update() 返回。
//
// 在函数内部尝试手动提交或回滚会导致 panic。
func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// 在 panic 的情况下确保事务回滚。
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// 标记为受管理的事务，以便内部函数不能手动提交。
	t.managed = true

	// 如果函数返回错误则回滚并返回该错误。
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

// View 在受管理的只读事务上下文中执行一个函数。
// 函数返回的任何错误都会由 View() 返回。
//
// 在函数内部尝试手动回滚会导致 panic。
func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// 在 panic 的情况下确保事务回滚。
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// 标记为受管理的事务，以便内部函数不能手动回滚。
	t.managed = true

	// 如果函数返回错误则传回该错误。
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Rollback()
}

// Batch 将 fn 作为批处理的一部分调用。它的行为类似于 Update，但：
//
// 1. 并发的 Batch 调用可以合并为单个 Bolt 事务。
//
// 2. 传入的函数可能会被调用多次（无论是否返回错误）。
//
// 这意味着 Batch 内函数的副作用必须是幂等的，并且只有在调用者看到成功返回时才产生永久效果。
//
// 最大批量大小和延迟可以通过 DB.MaxBatchSize 和 DB.MaxBatchDelay 调整。
//
// Batch 仅在多个 goroutine 调用时有用。
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// 没有现有批次，或现有批次已满；启动一个新的批次。
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// 唤醒批次，它已准备就绪运行
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

// trigger 运行批次（如果尚未运行）。
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run 执行批次中的事务并将结果传回 DB.Batch。
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// 确保不会向此批次添加新工作，但不影响其他批次。
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// 将失败的事务从批次中移除。这里缩短 b.calls 是安全的，因为 db.batch 不再指向我们，并且我们持有互斥锁。
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// 通知提交者单独重试，继续处理批次的其余部分
			c.err <- trySolo
			continue retry
		}

		// 将成功或 bolt 内部错误传递给所有调用方
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

// trySolo 是用于通知事务函数应单独重运行的特殊哨兵错误值。
// 调用方不应看到它。
var trySolo = errors.New("batch function returned an error and should be re-ran solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Sync 对数据库文件句柄执行 fdatasync()。
//
// 在正常操作下这不是必要的，但如果你使用 NoSync，则可以强制将数据库文件同步到磁盘。
func (db *DB) Sync() (err error) {
	if lg := db.Logger(); lg != discardLogger {
		lg.Debugf("Syncing bbolt db (%s)", db.path)
		defer func() {
			if err != nil {
				lg.Errorf("[GOOS: %s, GOARCH: %s] syncing bbolt db (%s) failed: %v", runtime.GOOS, runtime.GOARCH, db.path, err)
			} else {
				lg.Debugf("Syncing bbolt db (%s) successfully", db.path)
			}
		}()
	}

	return fdatasync(db)
}

// Stats 获取数据库的运行时性能统计。
// 只有在事务关闭时才更新这些数据。
func (db *DB) Stats() Stats {
	var s Stats
	if db.stats != nil {
		db.statlock.RLock()
		s = *db.stats
		db.statlock.RUnlock()
	}
	return s
}

// 这是为 C cursor 提供对原始数据字节的内部访问，谨慎使用，或者根本不要使用。
func (db *DB) Info() *Info {
	common.Assert(db.data != nil, "database file isn't correctly mapped")
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

// page 根据当前页面大小从 mmap 中检索页面引用。
func (db *DB) page(id common.Pgid) *common.Page {
	pos := id * common.Pgid(db.pageSize)
	return (*common.Page)(unsafe.Pointer(&db.data[pos]))
}

// pageInBuffer 根据当前页面大小从给定字节数组中检索页面引用。
func (db *DB) pageInBuffer(b []byte, id common.Pgid) *common.Page {
	return (*common.Page)(unsafe.Pointer(&b[id*common.Pgid(db.pageSize)]))
}

// meta 检索当前的 meta 页面引用。
func (db *DB) meta() *common.Meta {
	// 我们必须返回具有最高 txid 且验证不过的 meta。否则，当实际上数据库处于一致状态时会导致错误。metaA 是 txid 更高的那个。
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.Txid() > db.meta0.Txid() {
		metaA = db.meta1
		metaB = db.meta0
	}

	// 使用有效的较高 meta 页面。否则回退到较早的（如果有效）。
	if err := metaA.Validate(); err == nil {
		return metaA
	} else if err := metaB.Validate(); err == nil {
		return metaB
	}

	// 这不应该被触及，因为 meta1 和 meta0 在 mmap() 时已经验证过并且我们在每次写入时都 fsync()。
	panic("bolt.DB.meta(): invalid meta pages")
}

// allocate 返回从给定页面开始的一段连续内存。
func (db *DB) allocate(txid common.Txid, count int) (*common.Page, error) {
	// 为页面分配一个临时缓冲区。
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*common.Page)(unsafe.Pointer(&buf[0]))
	p.SetOverflow(uint32(count - 1))

	// 如果可用，则使用 freelist 的页面。
	p.SetId(db.freelist.Allocate(txid, count))
	if p.Id() != 0 {
		return p, nil
	}

	// 如果在末尾则调整 mmap 大小。
	p.SetId(db.rwtx.meta.Pgid())
	var minsz = int((p.Id()+common.Pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			if err == berrors.ErrMaxSizeReached {
				return nil, err
			} else {
				return nil, fmt.Errorf("mmap allocate error: %s", err)
			}
		}
	}

	// 移动页面 id 的高水位标记。
	curPgid := db.rwtx.meta.Pgid()
	db.rwtx.meta.SetPgid(curPgid + common.Pgid(count))

	return p, nil
}

// grow 将数据库大小增长到给定的 sz。
func (db *DB) grow(sz int) error {
	// 如果新大小小于可用文件大小则忽略。
	lg := db.Logger()
	fileSize, err := db.fileSize()
	if err != nil {
		lg.Errorf("getting file size failed: %w", err)
		return err
	}
	if sz <= fileSize {
		return nil
	}

	// 如果数据小于 alloc size 则仅分配所需大小。
	// 一旦超过分配大小则以块方式分配。
	if db.datasz <= db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	if !db.readOnly && db.MaxSize > 0 && sz > db.MaxSize {
		lg.Errorf("[GOOS: %s, GOARCH: %s] maximum db size reached, size: %d, db.MaxSize: %d", runtime.GOOS, runtime.GOARCH, sz, db.MaxSize)
		return berrors.ErrMaxSizeReached
	}

	// 截断并 fsync 以确保文件大小元数据被刷新。
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			// gofail: var resizeFileError string
			// return errors.New(resizeFileError)
			if err := db.file.Truncate(int64(sz)); err != nil {
				lg.Errorf("[GOOS: %s, GOARCH: %s] truncating file failed, size: %d, db.datasz: %d, error: %v", runtime.GOOS, runtime.GOARCH, sz, db.datasz, err)
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			lg.Errorf("[GOOS: %s, GOARCH: %s] syncing file failed, db.datasz: %d, error: %v", runtime.GOOS, runtime.GOARCH, db.datasz, err)
			return fmt.Errorf("file sync error: %s", err)
		}
		if db.Mlock {
			// 解锁旧文件并锁定新文件
			if err := db.mrelock(fileSize, sz); err != nil {
				return fmt.Errorf("mlock/munlock error: %s", err)
			}
		}
	}

	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

func (db *DB) freepages() []common.Pgid {
	tx, err := db.beginTx()
	defer func() {
		err = tx.Rollback()
		if err != nil {
			panic("freepages: failed to rollback tx")
		}
	}()
	if err != nil {
		panic("freepages: failed to open read only tx")
	}

	reachable := make(map[common.Pgid]*common.Page)
	nofreed := make(map[common.Pgid]bool)
	ech := make(chan error)
	go func() {
		for e := range ech {
			panic(fmt.Sprintf("freepages: failed to get all reachable pages (%v)", e))
		}
	}()
	tx.recursivelyCheckBucket(&tx.root, reachable, nofreed, HexKVStringer(), ech)
	close(ech)

	// TODO: 如果 check bucket 报告任何损坏（ech），我们不应该继续释放这些页面。

	var fids []common.Pgid
	for i := common.Pgid(2); i < db.meta().Pgid(); i++ {
		if _, ok := reachable[i]; !ok {
			fids = append(fids, i)
		}
	}
	return fids
}

func newFreelist(freelistType FreelistType) fl.Interface {
	if freelistType == FreelistMapType {
		return fl.NewHashMapFreelist()
	}
	return fl.NewArrayFreelist()
}

// Options 表示在打开数据库时可以设置的选项。
type Options struct {
	// Timeout 是等待获取文件锁的时间。
	// 设置为零将无限等待。
	Timeout time.Duration

	// 在内存映射文件之前设置 DB.NoGrowSync 标志。
	NoGrowSync bool

	// 不将 freelist 同步到磁盘。这在正常操作下提高数据库写性能，
	// 但在恢复期间需要完整的数据库重新同步。
	NoFreelistSync bool

	// PreLoadFreelist 设置在打开 db 文件时是否加载空闲页面。注意在以写模式打开 db 时，bbolt 会始终加载空闲页面。
	PreLoadFreelist bool

	// FreelistType 设置后端 freelist 类型。有两种选择。Array（数组）简单但当数据库很大且 freelist 分散时会出现显著性能下降。
	// 另一种是使用 hashmap，几乎在所有情况下都更快，但不能保证返回最小的可用页 id。通常是安全的。
	// 默认类型为 array
	FreelistType FreelistType

	// 以只读模式打开数据库。使用 flock(..., LOCK_SH |LOCK_NB) 来抓取共享锁（UNIX）。
	ReadOnly bool

	// 在内存映射文件之前设置 DB.MmapFlags 标志。
	MmapFlags int

	// InitialMmapSize 是数据库的初始 mmap 大小（字节）。
	// 如果 InitialMmapSize 足够大，则读事务不会阻塞写事务。（关于更多信息见 DB.Begin）
	//
	// 如果 <=0，则初始映射大小为 0。
	// 如果 initialMmapSize 小于以前的数据库大小，则不生效。
	//
	// 注意：在 Windows 上，由于平台限制，打开 DB 时数据库文件大小会立即调整为 `InitialMmapSize`（对齐到页面大小）。
	// 在非 Windows 平台上，文件大小会根据实际写入的数据动态增长，与 `InitialMmapSize` 无关。
	// 详见 https://github.com/etcd-io/bbolt/issues/378#issuecomment-1378121966。
	InitialMmapSize int

	// PageSize 覆盖默认的操作系统页面大小。
	PageSize int

	// MaxSize 设置数据文件的最大大小。<=0 表示没有最大值。
	MaxSize int

	// NoSync 在初始时设置 DB.NoSync。通常可以在 Open() 返回的 DB 上直接设置此值，
	// 但是该选项在暴露 Options 而不暴露底层 DB 的 API 中是有用的。
	NoSync bool

	// OpenFile 用于打开文件。默认为 os.OpenFile。此选项对编写纯净测试很有用。
	OpenFile func(string, int, os.FileMode) (*os.File, error)

	// Mlock 在设置为 true 时将数据库文件锁定到内存中。
	// 它可以防止潜在的页面错误，但被使用的内存无法回收。（仅 UNIX）
	Mlock bool

	// Logger 是 bbolt 使用的日志记录器。
	Logger Logger

	// NoStatistics 关闭统计收集，此时 Stats 方法将返回空结构体。
	// 在高并发只读事务下这可以提高性能。
	NoStatistics bool
}

func (o *Options) String() string {
	if o == nil {
		return "{}"
	}

	return fmt.Sprintf("{Timeout: %s, NoGrowSync: %t, NoFreelistSync: %t, PreLoadFreelist: %t, FreelistType: %s, ReadOnly: %t, MmapFlags: %x, InitialMmapSize: %d, PageSize: %d, MaxSize: %d, NoSync: %t, OpenFile: %p, Mlock: %t, Logger: %p, NoStatistics: %t}",
		o.Timeout, o.NoGrowSync, o.NoFreelistSync, o.PreLoadFreelist, o.FreelistType, o.ReadOnly, o.MmapFlags, o.InitialMmapSize, o.PageSize, o.MaxSize, o.NoSync, o.OpenFile, o.Mlock, o.Logger, o.NoStatistics)

}

// DefaultOptions 表示在传入 nil options 到 Open() 时使用的选项。
// 不使用超时，这将导致 Bolt 无限等待锁。
var DefaultOptions = &Options{
	Timeout:      0,
	NoGrowSync:   false,
	FreelistType: FreelistArrayType,
}

// Stats 表示有关数据库的统计信息。
type Stats struct {
	// 将 `TxStats` 放在第一个字段以确保 64 位对齐。
	// 注意分配的结构体的第一个字是可以保证 64 位对齐的。
	// 详见 https://pkg.go.dev/sync/atomic#pkg-note-BUG。
	// 另见 https://github.com/etcd-io/bbolt/issues/577 中的讨论。
	TxStats TxStats // 全局、正在进行的统计。

	// Freelist 统计
	FreePageN     int // freelist 上的空闲页面总数
	PendingPageN  int // freelist 上的挂起页面总数
	FreeAlloc     int // 空闲页面中分配的总字节数
	FreelistInuse int // freelist 占用的总字节数

	// 事务统计
	TxN     int // 启动的只读事务总数
	OpenTxN int // 当前打开的只读事务数量
}

// Sub 计算并返回两个数据库统计的差异。
// 当在不同时间点获取统计并需要该时间段内的性能计数时很有用。
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

type Info struct {
	Data     uintptr
	PageSize int
}
