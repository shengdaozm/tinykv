package engine_util

/*
Engine（引擎）是一个用于在本地存储键/值对的低级系统（没有分布式或任何事务支持等）。
本包包含用于与此类引擎交互的代码。

CF 的意思是 'column family'（列族）。https://github.com/facebook/rocksdb/wiki/Column-Families
提供了一个关于列族的很好描述（专门针对 RocksDB，但一般概念是通用的）。
简而言之，列族是一个键命名空间。多个列族通常实现为几乎独立的数据库。
重要的是，每个列族可以单独配置。写入可以在列族之间实现原子化，这对于单独的数据库是无法做到的。

engine_util 包括以下包：

* engines：用于保存 unistore 所需引擎的数据结构。
* write_batch：将写入分批处理为单个原子“事务”的代码。
* cf_iterator：用于在 badger 中迭代整个列族的代码。
*/
