package com.grok.raft.effects

import com.grok.raft.core.KeyValueRaft
import com.grok.raft.effects.storage.RocksDBKeyValueStorage
import cats.effect.Async
import cats.implicits.*
import org.typelevel.log4cats.Logger

/**
 * Factory for creating KeyValueRaft instances backed by RocksDB storage.
 */
object KeyValueRaftRocksDB {

  /**
   * Creates a new KeyValueRaft instance with RocksDB storage.
   * 
   * @param dbPath Path to the RocksDB database directory
   * @return A KeyValueRaft instance backed by RocksDB
   */
  def create[F[_]: Async: Logger](
    dbPath: String
  ): F[KeyValueRaft[F]] = {
    for {
      // Create RocksDB storage
      storage <- RocksDBKeyValueStorage.create[F](dbPath)
      
      // Create KeyValueRaft with RocksDB storage
      kvRaft <- KeyValueRaft.create[F](storage)
    } yield kvRaft
  }

  /**
   * Creates a new KeyValueRaft instance with RocksDB storage and resource management.
   * The storage will be automatically closed when the resource is released.
   * 
   * @param dbPath Path to the RocksDB database directory
   * @return A Resource containing a KeyValueRaft instance backed by RocksDB
   */
  def createResource[F[_]: Async: Logger](
    dbPath: String
  ): cats.effect.Resource[F, KeyValueRaft[F]] = {
    cats.effect.Resource.make(
      create[F](dbPath)
    )(_.close())
  }
}