package com.grok.raft.effects.storage

import cats.effect.*
import cats.syntax.all.*
import com.grok.raft.core.internal.LogEntry
import com.grok.raft.core.internal.storage.LogStorage
import com.grok.raft.effects.storage.BinarySerializer
import java.nio.file.Path
import java.nio.ByteBuffer

class RocksDBLogStorage[F[_]: Sync] private (
    private val rocksDB: RocksDBWrapper[F]
) extends LogStorage[F] {

  private val LOGS_CF        = "logs"
  private val LAST_INDEX_KEY = "last_index".getBytes("UTF-8")

  override def lastIndex: F[Long] =
    rocksDB.get(LAST_INDEX_KEY, LOGS_CF).map {
      case Some(bytes) => ByteBuffer.wrap(bytes).getLong()
      case None        => 0L
    }

  override def get(index: Long): F[Option[LogEntry]] =
    rocksDB.get(indexToBytes(index), LOGS_CF).map {
      _.map(BinarySerializer.deserializeLogEntry)
    }

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    for {
      serialized <- Sync[F].pure(BinarySerializer.serializeLogEntry(logEntry))
      _          <- rocksDB.put(indexToBytes(index), serialized, LOGS_CF)
      _          <- updateLastIndex(index)
    } yield logEntry

  override def deleteBefore(index: Long): F[Unit] =
    for {
      lastIdx <- lastIndex
      _ <-
        if (index > 0 && index <= lastIdx) {
          deleteRange(1L, index)
        } else Sync[F].unit
    } yield ()

  override def deleteAfter(index: Long): F[Unit] =
    for {
      lastIdx <- lastIndex
      _ <-
        if (index < lastIdx) {
          deleteRange(index + 1, lastIdx)
        } else Sync[F].unit
      _ <- updateLastIndex(index)
    } yield ()

  def appendEntries(entries: List[LogEntry], startIndex: Long): F[Unit] = {
    val operations = entries.zipWithIndex.map { case (entry, i) =>
      val index      = startIndex + i
      val serialized = BinarySerializer.serializeLogEntry(entry)
      BatchPut(indexToBytes(index), serialized, LOGS_CF)
    }

    val lastIndex   = startIndex + entries.length - 1
    val lastIndexOp = BatchPut(LAST_INDEX_KEY, longToBytes(lastIndex), LOGS_CF)

    rocksDB.writeBatch(operations :+ lastIndexOp)
  }

  def getRange(startIndex: Long, endIndex: Long): F[List[LogEntry]] =
    Resource.make(rocksDB.newIterator(LOGS_CF))(iter => Sync[F].blocking(iter.close())).use { iterator =>
      Sync[F].blocking {
        iterator.seek(indexToBytes(startIndex))
        val entries = scala.collection.mutable.ListBuffer[LogEntry]()

        while (iterator.isValid && bytesToIndex(iterator.key()) <= endIndex) {
          val entry = BinarySerializer.deserializeLogEntry(iterator.value())
          entries += entry
          iterator.next()
        }

        entries.toList
      }
    }

  private def updateLastIndex(index: Long): F[Unit] =
    rocksDB.put(LAST_INDEX_KEY, longToBytes(index), LOGS_CF)

  private def deleteRange(startIndex: Long, endIndex: Long): F[Unit] = {
    val operations = (startIndex to endIndex).map { index =>
      BatchDelete(indexToBytes(index), LOGS_CF)
    }.toList

    rocksDB.writeBatch(operations)
  }

  private def indexToBytes(index: Long): Array[Byte] =
    longToBytes(index)

  private def longToBytes(value: Long): Array[Byte] =
    ByteBuffer.allocate(8).putLong(value).array()

  private def bytesToIndex(bytes: Array[Byte]): Long =
    ByteBuffer.wrap(bytes).getLong()
}

object RocksDBLogStorage {

  def apply[F[_]: Sync](path: Path): Resource[F, RocksDBLogStorage[F]] =
    RocksDBWrapper[F](path).map(new RocksDBLogStorage[F](_))
}