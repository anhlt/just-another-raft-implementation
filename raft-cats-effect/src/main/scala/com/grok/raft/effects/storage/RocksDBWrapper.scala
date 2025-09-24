package com.grok.raft.effects.storage

import cats.effect.*
import cats.syntax.all.*
import org.rocksdb.*
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*
import scala.util.Try

class RocksDBWrapper[F[_]: Sync] private (
    private val db: RocksDB,
    private val columnFamilies: Map[String, ColumnFamilyHandle],
    private val options: DBOptions
) {

  def get(key: Array[Byte], columnFamily: String = "default"): F[Option[Array[Byte]]] =
    Sync[F].blocking {
      Option(db.get(columnFamilies(columnFamily), key))
    }

  def put(key: Array[Byte], value: Array[Byte], columnFamily: String = "default"): F[Unit] =
    Sync[F].blocking {
      db.put(columnFamilies(columnFamily), key, value)
    }

  def delete(key: Array[Byte], columnFamily: String = "default"): F[Unit] =
    Sync[F].blocking {
      db.delete(columnFamilies(columnFamily), key)
    }

  def writeBatch(operations: List[BatchOperation]): F[Unit] =
    Sync[F].blocking {
      val batch = new WriteBatch()
      try {
        operations.foreach {
          case BatchPut(key, value, cf) =>
            batch.put(columnFamilies(cf), key, value)
          case BatchDelete(key, cf) =>
            batch.delete(columnFamilies(cf), key)
        }
        db.write(new WriteOptions(), batch)
      } finally {
        batch.close()
      }
    }

  def newIterator(columnFamily: String = "default"): F[RocksIterator] =
    Sync[F].blocking {
      db.newIterator(columnFamilies(columnFamily))
    }

  def close(): F[Unit] =
    Sync[F].blocking {
      columnFamilies.values.foreach(_.close())
      db.close()
      options.close()
    }
}

sealed trait BatchOperation
case class BatchPut(key: Array[Byte], value: Array[Byte], columnFamily: String = "default") extends BatchOperation
case class BatchDelete(key: Array[Byte], columnFamily: String = "default")                  extends BatchOperation

object RocksDBWrapper {

  def apply[F[_]: Sync](
      path: Path,
      columnFamilyNames: List[String] = List("default", "logs", "state", "snapshots")
  ): Resource[F, RocksDBWrapper[F]] = {

    val acquire = Sync[F].blocking {
      RocksDB.loadLibrary()

      val options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setMaxBackgroundJobs(4)
        .setDbWriteBufferSize(64 * 1024 * 1024)

      val cfOptions = new ColumnFamilyOptions()
        .setCompressionType(CompressionType.LZ4_COMPRESSION)
        .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)

      val cfDescriptors = columnFamilyNames.map { name =>
        new ColumnFamilyDescriptor(name.getBytes, cfOptions)
      }.asJava

      val cfHandles = new java.util.ArrayList[ColumnFamilyHandle]()

      val db = RocksDB.open(options, path.toString, cfDescriptors, cfHandles)

      val cfMap = columnFamilyNames.zip(cfHandles.asScala.toList).toMap

      new RocksDBWrapper(db, cfMap, options)
    }

    val release = (wrapper: RocksDBWrapper[F]) => wrapper.close()

    Resource.make(acquire)(release)
  }
}