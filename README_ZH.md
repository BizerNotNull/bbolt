bbolt
=====

`bbolt` 是 [Ben Johnson 的][gh_ben] [Bolt][bolt] 键值存储的社区维护分支，目标是为 Go 生态提供一个稳定、持续更新的嵌入式数据库。它保持与 Bolt API 的向后兼容，同时带来修复和性能改进。

[gh_ben]: https://github.com/benbjohnson
[bolt]: https://github.com/boltdb/bolt
[hyc_symas]: https://twitter.com/hyc_symas
[lmdb]: https://www.symas.com/symas-embedded-database-lmdb

## 项目状态

Bolt/bbolt 的文件格式和 API 均已稳定，拥有完善的单元测试与随机黑盒测试，已在高负载生产环境中使用（数据库体积可达 TB 级）。 Shopify、Heroku 等公司在生产中使用基于 Bolt 的服务。

## 版本与兼容性

- 遵循 [语义化版本](http://semver.org)。
- 补丁版、次版本不会破坏 API；次版本可能新增特性。

## 目录

- [快速开始](#快速开始)
  - [安装](#安装)
  - [导入并打开数据库](#导入并打开数据库)
  - [事务模型](#事务模型)
  - [Bucket 操作](#bucket-操作)
  - [键值读写](#键值读写)
  - [自增序列](#自增序列)
  - [遍历键](#遍历键)
  - [嵌套 Bucket](#嵌套-bucket)
  - [批量写入](#批量写入)
  - [只读模式](#只读模式)
  - [备份](#备份)
  - [移动端使用](#移动端使用-iosandroid)
- [与其他数据库对比](#与其他数据库对比)
- [注意事项与限制](#注意事项与限制)
- [源码导读](#源码导读)
- [已知问题](#已知问题)
- [更多资源](#更多资源)

## 快速开始

### 安装

安装库：

```sh
go get go.etcd.io/bbolt@latest
```

安装命令行工具：

```sh
go install go.etcd.io/bbolt/cmd/bbolt@latest
```

### 导入并打开数据库

```go
import bolt "go.etcd.io/bbolt"

db, err := bolt.Open("my.db", 0600, nil)
if err != nil {
	log.Fatal(err)
}
defer db.Close()
```

`Open` 会为数据文件加锁，避免多个进程同时读写。可通过 `Options.Timeout` 设定等待锁的超时时间：

```go
db, err := bolt.Open("my.db", 0600, &bolt.Options{Timeout: time.Second})
```

### 事务模型

- 同时只允许一个读写事务，读事务可以并发多个。
- 事务及其派生对象（Bucket、游标等）不应在多个 goroutine 间共享。
- 长时间持有读事务会阻塞写事务的页面重映射。

开启读写事务：

```go
err := db.Update(func(tx *bolt.Tx) error {
	// 读写操作
	return nil
})
```

读事务：

```go
err := db.View(func(tx *bolt.Tx) error {
	// 只读操作
	return nil
})
```

如需手动管理生命周期，可使用 `Begin` 并在结束时 `Commit` 或 `Rollback`：

```go
tx, err := db.Begin(true)
if err != nil {
	return err
}
defer tx.Rollback()

// ...操作...

if err := tx.Commit(); err != nil {
	return err
}
```

### Bucket 操作

Bucket 是数据库中的键值集合，键唯一。常用方法：

```go
// 创建（若不存在则创建）
b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))

// 获取已存在的 Bucket
b := tx.Bucket([]byte("MyBucket"))

// 删除 Bucket
err := tx.DeleteBucket([]byte("MyBucket"))

// 遍历顶层 Bucket
tx.ForEach(func(name []byte, b *bolt.Bucket) error {
	fmt.Println(string(name))
	return nil
})
```

### 键值读写

```go
// 写入
db.Update(func(tx *bolt.Tx) error {
	b := tx.Bucket([]byte("MyBucket"))
	return b.Put([]byte("answer"), []byte("42"))
})

// 读取
db.View(func(tx *bolt.Tx) error {
	v := tx.Bucket([]byte("MyBucket")).Get([]byte("answer"))
	fmt.Printf("The answer is: %s\n", v)
	return nil
})

// 删除
db.Update(func(tx *bolt.Tx) error {
	return tx.Bucket([]byte("MyBucket")).Delete([]byte("answer"))
})
```

从 `Get` 返回的切片仅在事务存活期间有效，如需持久化请 `copy`。

### 自增序列

`Bucket.NextSequence()` 可生成自增 ID：

```go
id, _ := b.NextSequence()
```

### 遍历键

键按字节序排序，使用游标高效遍历：

```go
c := b.Cursor()
for k, v := c.First(); k != nil; k, v = c.Next() {
	fmt.Printf("key=%s, value=%s\n", k, v)
}
```

前缀/范围扫描示例：

```go
// 前缀
for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
	// ...
}

// 范围
for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
	// ...
}
```

若值为 `nil` 且键非空，表示该键对应子 Bucket，可用 `Bucket.Bucket()` 获取。

### 嵌套 Bucket

Bucket 可作为值存储形成嵌套结构，例如为每个账号单独维护子 Bucket：

```go
root := tx.Bucket([]byte(strconv.FormatUint(accountID, 10)))
users, _ := root.CreateBucketIfNotExists([]byte("USERS"))
userID, _ := users.NextSequence()
// ...写入用户数据...
```

### 批量写入

`DB.Batch` 会把并发的更新合并为更大的事务，减少 fsync 次数：

```go
err := db.Batch(func(tx *bolt.Tx) error {
	// 函数需具备幂等性
	return nil
})
```

### 只读模式

通过 `Options.ReadOnly` 打开数据库，可在只读场景避免写锁：

```go
db, err := bolt.Open("my.db", 0400, &bolt.Options{ReadOnly: true})
```

### 备份

在读事务中使用 `Tx.WriteTo` 可在线热备份，不阻塞其他读写：

```go
err := db.View(func(tx *bolt.Tx) error {
	f, _ := os.Create("backup.db")
	defer f.Close()
	_, err := tx.WriteTo(f)
	return err
})
```

### 移动端使用 (iOS/Android)

可通过 gomobile 构建绑定。示例请参考英文 README 中的 `cmd/bbolt` 及移动端代码片段。移动端通常需要将数据库文件放置在不会被云同步的目录，并在关闭时调用 `Close`。

## 与其他数据库对比

- **Postgres/MySQL 等关系型数据库**：关系型提供 SQL 与灵活查询，但需要进程间通信；bbolt 内嵌在进程中，按键读写更快但不支持 SQL/多进程共享。
- **LevelDB/RocksDB 等 LSM 引擎**：LSM 擅长高随机写吞吐；bbolt 使用 B+ 树，擅长读密集与范围扫描，且提供可串行化事务。
- **LMDB**：架构相似（B+ 树、单写多读、MVCC），LMDB 更偏性能，bbolt 更注重安全与易用，自动管理 mmap 扩容。

## 注意事项与限制

- 适合读多写少场景，随机写可通过 `Batch` 或 WAL 优化。
- 长时间读事务会阻止页面回收，尽量缩短。
- 返回的切片仅在事务内有效。
- 数据文件独占写锁，无法被多个进程同时写入。
- 大量随机写入新 Bucket 时，单事务超过 ~100k 条会较慢。
- 文件为内存映射，跨大小端平台拷贝文件不可行。
- 删除大量数据不会收缩文件大小，空间会保留在 freelist 中供后续复用。

更完整的限制列表可参考英文 README。

## 源码导读

核心入口可从以下函数/类型入手：

- `Open`：初始化数据库文件、加锁、读取元信息、mmap 文件。
- `DB.Begin`：创建读/写事务，并维护元数据锁与写锁。
- `Bucket.Put`/`Bucket.Get`：通过游标定位页面，写入或读取键值。
- `Cursor`：在 B+ 树上前后移动、定位键。
- `Tx.Commit`：将脏节点与 freelist 转化为页面并两阶段刷盘（数据页 -> `fsync` -> meta 页 -> `fsync`），确保崩溃后一致性。

## 已知问题

- Linux 在启用 ext4 fast commit（5.10 引入）且未升级至 5.17 的内核上可能导致数据损坏，详见英文 README 中的链接与讨论。
- 写入长度为 0 的值，读取时会返回空的 `[]byte{}`（issue #726）。

## 更多资源

- [Intro to BoltDB: Painless Performant Persistence](http://npf.io/2014/07/intro-to-boltdb-painless-performant-persistence/)
- [Bolt -- an embedded key/value database for Go](https://www.progville.com/go/bolt-embedded-db-golang/)

如需更完整的 API 细节、对比及社区项目列表，请参阅英文版 `README.md`。
