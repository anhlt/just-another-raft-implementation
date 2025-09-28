package com.grok.raft.effects.storage

import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.grok.raft.core.internal.storage.LogStorage
import com.grok.raft.core.internal.{LogEntry}
import com.grok.raft.effects.storage.codec.RaftCodecs
import org.rocksdb.*
import java.nio.ByteBuffer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*
import scala.jdk.CollectionConverters.*

class RocksDBLogStorage[F[_]: Async: Logger] private (
    val db: RocksDB,
    val logCF: ColumnFamilyHandle,
    val metaCF: ColumnFamilyHandle,
    val writeOptions: WriteOptions
) extends LogStorage[F]:

  private val LAST_INDEX_KEY = "last_index".getBytes
  private val FIRST_INDEX_KEY = "first_index".getBytes
  private val SNAPSHOT_INDEX_KEY = "snapshot_index".getBytes

  private def logKey(index: Long): Array[Byte] =
    f"log:$index%010d".getBytes

  private def longToBytes(value: Long): Array[Byte] =
    ByteBuffer.allocate(8).putLong(value).array()

  private def bytesToLong(bytes: Array[Byte]): Long =
    if bytes == null then 0L
    else ByteBuffer.wrap(bytes).getLong()

  override def lastIndex: F[Long] =
    for
      _ <- trace"Getting last index from RocksDB"
      bytes <- Sync[F].delay(db.get(metaCF, LAST_INDEX_KEY))
      result = bytesToLong(bytes)
      _ <- trace"Last index: $result"
    yield result

  override def get(index: Long): F[Option[LogEntry]] =
    for
      _ <- trace"Getting log entry at index $index"
      key = logKey(index)
      bytes <- Sync[F].delay(db.get(logCF, key))
      result <- Option(bytes) match
        case None => 
          trace"Log entry at index $index not found" *> none[LogEntry].pure[F]
        case Some(data) =>
          RaftCodecs.decodeLogEntry(data) match
            case Right(entry) => 
              trace"Retrieved log entry at index $index: $entry" *> entry.some.pure[F]
            case Left(err) => 
              error"Failed to decode log entry at index $index: $err" *> 
              Sync[F].raiseError(new RuntimeException(s"Failed to decode log entry: $err"))
    yield result

  override def put(index: Long, logEntry: LogEntry): F[LogEntry] =
    for
      _ <- trace"Storing log entry at index $index: $logEntry"
      key = logKey(index)
      encodedEntry <- RaftCodecs.encodeLogEntry(logEntry) match
        case Right(bytes) => bytes.pure[F]
        case Left(err) => Sync[F].raiseError(new RuntimeException(s"Failed to encode log entry: $err"))
      
      writeBatch <- Sync[F].delay(new WriteBatch())
      _ <- Sync[F].delay {
        writeBatch.put(logCF, key, encodedEntry)
        writeBatch.put(metaCF, LAST_INDEX_KEY, longToBytes(index))
      }
      _ <- Sync[F].delay(db.write(writeOptions, writeBatch))
      _ <- Sync[F].delay(writeBatch.close())
      _ <- trace"Successfully stored log entry at index $index"
    yield logEntry

  override def deleteBefore(index: Long): F[Unit] =
    for
      _ <- trace"Deleting log entries before index $index"
      firstIndex <- getFirstIndex()
      writeBatch <- Sync[F].delay(new WriteBatch())
      _ <- (firstIndex until index).toList.traverse_ { i =>
        Sync[F].delay(writeBatch.delete(logCF, logKey(i)))
      }
      _ <- Sync[F].delay {
        writeBatch.put(metaCF, FIRST_INDEX_KEY, longToBytes(index))
        writeBatch.put(metaCF, SNAPSHOT_INDEX_KEY, longToBytes(index - 1))
      }
      _ <- Sync[F].delay(db.write(writeOptions, writeBatch))
      _ <- Sync[F].delay(writeBatch.close())
      _ <- trace"Deleted log entries before index $index"
    yield ()

  override def deleteAfter(index: Long): F[Unit] =
    for
      _ <- trace"Deleting log entries after index $index"
      lastIdx <- lastIndex
      writeBatch <- Sync[F].delay(new WriteBatch())
      _ <- ((index + 1) to lastIdx).toList.traverse_ { i =>
        Sync[F].delay(writeBatch.delete(logCF, logKey(i)))
      }
      _ <- Sync[F].delay(writeBatch.put(metaCF, LAST_INDEX_KEY, longToBytes(index)))
      _ <- Sync[F].delay(db.write(writeOptions, writeBatch))
      _ <- Sync[F].delay(writeBatch.close())
      _ <- trace"Deleted log entries after index $index"
    yield ()

  private def getFirstIndex(): F[Long] =
    for
      bytes <- Sync[F].delay(db.get(metaCF, FIRST_INDEX_KEY))
      result = if bytes == null then 1L else bytesToLong(bytes)
    yield result

object RocksDBLogStorage:

  def create[F[_]: Async: Logger](
      config: RocksDBConfig
  ): Resource[F, RocksDBLogStorage[F]] =
    
    val columnFamilyDescriptors = List(
      new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, config.createColumnFamilyOptions()),
      new ColumnFamilyDescriptor("raft_log".getBytes, config.createColumnFamilyOptions()),
      new ColumnFamilyDescriptor("log_meta".getBytes, config.createColumnFamilyOptions())
    )

    Resource.make {
      for
        _ <- info"Opening RocksDB log storage at ${config.logDbPath}"
        _ <- Sync[F].delay(java.nio.file.Files.createDirectories(config.logDbPath))
        
        columnFamilyHandles <- Sync[F].delay(new java.util.ArrayList[ColumnFamilyHandle]())
        db <- Sync[F].delay {
          RocksDB.open(
            config.createLogDBOptions(),
            config.logDbPath.toString,
            columnFamilyDescriptors.asJava,
            columnFamilyHandles
          )
        }
        
        defaultCF = columnFamilyHandles.get(0)
        logCF = columnFamilyHandles.get(1)
        metaCF = columnFamilyHandles.get(2)
        
        writeOptions <- Sync[F].delay {
          val opts = new WriteOptions()
          opts.setSync(true)
          opts
        }
        
        storage = new RocksDBLogStorage[F](db, logCF, metaCF, writeOptions)
        _ <- info"RocksDB log storage opened successfully"
      yield storage
    } { storage =>
      for
        _ <- info"Closing RocksDB log storage"
        _ <- Sync[F].delay(storage.writeOptions.close())
        _ <- Sync[F].delay(storage.metaCF.close())
        _ <- Sync[F].delay(storage.logCF.close())
        _ <- Sync[F].delay(storage.db.close())
        _ <- info"RocksDB log storage closed"
      yield ()
    }