package com.grok.raft.effects.storage

import com.grok.raft.core.storage.KeyValueStorage
import cats.effect.Sync
import org.rocksdb.*
import java.nio.file.Paths
import scala.collection.mutable
import scala.collection.immutable.Map

class RocksDBKeyValueStorage[F[_]: Sync] private (
  private val db: RocksDB,
  private val defaultCF: ColumnFamilyHandle,
  private val options: DBOptions
) extends KeyValueStorage[F] {

  override def get(key: String): F[Option[String]] = 
    Sync[F].blocking {
      Option(db.get(defaultCF, key.getBytes("UTF-8"))).map(new String(_, "UTF-8"))
    }

  override def put(key: String, value: String): F[Option[String]] = 
    Sync[F].blocking {
      val oldValue = Option(db.get(defaultCF, key.getBytes("UTF-8"))).map(new String(_, "UTF-8"))
      db.put(defaultCF, key.getBytes("UTF-8"), value.getBytes("UTF-8"))
      oldValue
    }

  override def remove(key: String): F[Option[String]] = 
    Sync[F].blocking {
      val oldValue = Option(db.get(defaultCF, key.getBytes("UTF-8"))).map(new String(_, "UTF-8"))
      db.delete(defaultCF, key.getBytes("UTF-8"))
      oldValue
    }

  override def scan(prefix: String): F[Map[String, String]] = 
    Sync[F].blocking {
      val iterator = db.newIterator(defaultCF)
      val result = mutable.Map[String, String]()
      
      try {
        iterator.seek(prefix.getBytes("UTF-8"))
        
        while (iterator.isValid && new String(iterator.key(), "UTF-8").startsWith(prefix)) {
          val key = new String(iterator.key(), "UTF-8")
          val value = new String(iterator.value(), "UTF-8")
          result += (key -> value)
          iterator.next()
        }
      } finally {
        iterator.close()
      }
      
      result.toMap
    }

  override def keys(): F[Set[String]] = 
    Sync[F].blocking {
      val iterator = db.newIterator(defaultCF)
      try {
        val result = mutable.Set[String]()
        iterator.seekToFirst()
        
        while (iterator.isValid) {
          val key = new String(iterator.key(), "UTF-8")
          result += key
          iterator.next()
        }
        result.toSet
      } finally {
        iterator.close()
      }
    }

  override def range(startKey: String, endKey: String): F[Map[String, String]] = 
    Sync[F].blocking {
      val iterator = db.newIterator(defaultCF)
      val result = mutable.Map[String, String]()
      
      try {
        iterator.seek(startKey.getBytes("UTF-8"))
        var continue = true
        
        while (iterator.isValid && continue) {
          val key = new String(iterator.key(), "UTF-8")
          if (key >= startKey && key < endKey) {
            val value = new String(iterator.value(), "UTF-8")
            result += (key -> value)
            iterator.next()
          } else if (key >= endKey) {
            // Keys are lexicographically ordered, so we can stop
            continue = false
          } else {
            iterator.next()
          }
        }
      } finally {
        iterator.close()
      }
      
      result.toMap
    }

  override def close(): F[Unit] = 
    Sync[F].blocking {
      defaultCF.close()
      db.close()
      options.close()
    }
}

object RocksDBKeyValueStorage {
  
  def create[F[_]: Sync](path: String): F[RocksDBKeyValueStorage[F]] = 
    Sync[F].blocking {
      RocksDB.loadLibrary()
      
      val options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setMaxBackgroundJobs(4)
        .setDbWriteBufferSize(64 * 1024 * 1024)
      
      val cfOptions = new ColumnFamilyOptions()
        .setCompressionType(CompressionType.LZ4_COMPRESSION)
        .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
      
      val cfDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions)
      val cfHandles = new java.util.ArrayList[ColumnFamilyHandle]()
      
      val pathObj = Paths.get(path)
      pathObj.getParent.toFile.mkdirs()
      
      val db = RocksDB.open(options, path, java.util.Arrays.asList(cfDescriptor), cfHandles)
      val defaultCF = cfHandles.get(0)
      
      new RocksDBKeyValueStorage[F](db, defaultCF, options)
    }
}