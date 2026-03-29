package com.grok.raft.effects.storage

import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.mtl.Raise
import com.grok.raft.core.protocol.{KVStateMachine, WriteCommand, ReadCommand}
import com.grok.raft.core.error.StateMachineError
import org.rocksdb.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*
import scala.jdk.CollectionConverters.*
import java.nio.ByteBuffer

class RocksDBKVStateMachine[F[_]: Async: Logger] private (
    val db: RocksDB,
    val kvDataCF: ColumnFamilyHandle,
    val writeOptions: WriteOptions,
    val readOptions: ReadOptions
)(using Raise[F, StateMachineError]) extends KVStateMachine[F]:

  private val APPLIED_INDEX_KEY = "__applied_index__".getBytes

  private def iteratorToStream(iterator: RocksIterator): LazyList[Array[Byte]] =
    if iterator.isValid then
      val key = iterator.key().clone()
      iterator.next()
      key #:: iteratorToStream(iterator)
    else LazyList.empty

  private def iteratorToKeyValueStream(iterator: RocksIterator): LazyList[(Array[Byte], Array[Byte])] =
    if iterator.isValid then
      val key = iterator.key().clone()
      val value = iterator.value().clone()
      iterator.next()
      (key, value) #:: iteratorToKeyValueStream(iterator)
    else LazyList.empty

  private def clearAllData(): F[Unit] =
    for
      _ <- trace"Clearing all data from state machine"
      iterator <- Sync[F].delay(db.newIterator(kvDataCF))
      _ <- Sync[F].delay {
        try
          iterator.seekToFirst()
          val writeBatch = new WriteBatch()
          
          iteratorToStream(iterator)
            .foldLeft(()) { (_, key) =>
              writeBatch.delete(kvDataCF, key)
            }
          
          db.write(writeOptions, writeBatch)
          writeBatch.close()
        finally
          iterator.close()
      }
      _ <- trace"Successfully cleared all data"
    yield ()

  override def put(key: Array[Byte], value: Array[Byte]): F[Unit] =
    for
      _ <- trace"Putting key-value pair: ${new String(key)} -> ${new String(value)}"
      _ <- Sync[F].delay(db.put(kvDataCF, writeOptions, key, value))
      _ <- trace"Successfully stored key-value pair"
    yield ()

  override def get(key: Array[Byte]): F[Option[Array[Byte]]] =
    for
      _ <- trace"Getting value for key: ${new String(key)}"
      value <- Sync[F].delay(db.get(kvDataCF, readOptions, key))
      result = Option(value)
      _ <- trace"Retrieved value: ${result.map(new String(_))}"
    yield result

  override def delete(key: Array[Byte]): F[Unit] =
    for
      _ <- trace"Deleting key: ${new String(key)}"
      _ <- Sync[F].delay(db.delete(kvDataCF, writeOptions, key))
      _ <- trace"Successfully deleted key"
    yield ()

  override def contains(key: Array[Byte]): F[Boolean] =
    for
      _ <- trace"Checking if key exists: ${new String(key)}"
      value <- Sync[F].delay(db.get(kvDataCF, readOptions, key))
      result = value != null
      _ <- trace"Key exists: $result"
    yield result

  override def range(
      startKey: Array[Byte], 
      endKey: Array[Byte], 
      limit: Option[Int] = None
  ): F[List[(Array[Byte], Array[Byte])]] =
    for
      _ <- trace"Range query from ${new String(startKey)} to ${new String(endKey)}, limit: $limit"
      iterator <- Sync[F].delay(db.newIterator(kvDataCF, readOptions))
      result <- Sync[F].delay {
        iterator.seek(startKey)
        
        val results = iteratorToKeyValueStream(iterator)
          .takeWhile { case (key, _) => java.util.Arrays.compare(key, endKey) < 0 }
          .take(limit.getOrElse(Int.MaxValue))
          .foldLeft(List.empty[(Array[Byte], Array[Byte])]) { (acc, keyValue) =>
            keyValue :: acc
          }
          .reverse
        
        iterator.close()
        results
      }
      _ <- trace"Range query returned ${result.size} results"
    yield result

  override def scan(
      prefix: Array[Byte], 
      limit: Option[Int] = None
  ): F[List[(Array[Byte], Array[Byte])]] =
    for
      _ <- trace"Scan with prefix: ${new String(prefix)}, limit: $limit"
      iterator <- Sync[F].delay(db.newIterator(kvDataCF, readOptions))
      result <- Sync[F].delay {
        iterator.seek(prefix)
        
        val results = iteratorToKeyValueStream(iterator)
          .takeWhile { case (key, _) => key.startsWith(prefix) }
          .take(limit.getOrElse(Int.MaxValue))
          .foldLeft(List.empty[(Array[Byte], Array[Byte])]) { (acc, keyValue) =>
            keyValue :: acc
          }
          .reverse
        
        iterator.close()
        results
      }
      _ <- trace"Scan returned ${result.size} results"
    yield result

  override def keys(
      prefix: Option[Array[Byte]] = None, 
      limit: Option[Int] = None
  ): F[List[Array[Byte]]] =
    for
      _ <- trace"Keys query with prefix: ${prefix.map(new String(_))}, limit: $limit"
      iterator <- Sync[F].delay(db.newIterator(kvDataCF, readOptions))
      result <- Sync[F].delay {
        prefix.foreach(iterator.seek)
        if prefix.isEmpty then iterator.seekToFirst()
        
        val results = iteratorToStream(iterator)
          .takeWhile(key => prefix.forall(p => key.startsWith(p)))
          .take(limit.getOrElse(Int.MaxValue))
          .foldLeft(List.empty[Array[Byte]]) { (acc, key) =>
            key :: acc
          }
          .reverse
        
        iterator.close()
        results
      }
      _ <- trace"Keys query returned ${result.size} results"
    yield result

  // StateMachine interface methods
  override def appliedIndex: F[Long] =
    for
      _ <- trace"Getting applied index from state machine"
      bytes <- Sync[F].delay(Option(db.get(kvDataCF, APPLIED_INDEX_KEY)))
      index <- bytes.fold(0L.pure[F]) { bytes =>
        ByteBuffer.wrap(bytes).getLong.pure[F]
      }
      _ <- trace"Applied index: $index"
    yield index

  override def getCurrentState: F[Map[Array[Byte], Array[Byte]]] =
    for
      _ <- trace"Getting current state from RocksDB"
      iterator <- Sync[F].delay(db.newIterator(kvDataCF))
      pairs <- Sync[F].delay {
        try 
          iterator.seekToFirst()
          
          iteratorToKeyValueStream(iterator)
            .filterNot { case (key, _) => key.startsWith(APPLIED_INDEX_KEY) }
            .foldLeft(Map.empty[Array[Byte], Array[Byte]]) { case (map, (key, value)) =>
              map + (key -> value)
            }
        finally
          iterator.close()
      }
      _ <- trace"Retrieved ${pairs.size} key-value pairs"
    yield pairs

  override def restoreSnapshot[T](lastIndex: Long, data: T): F[Unit] =
    data match
      case snapshot: Map[Array[Byte], Array[Byte]] @unchecked =>
        restoreFromSnapshot(snapshot, lastIndex)
      case _ =>
        corruptedState("Invalid snapshot data type")

  def restoreFromSnapshot(snapshot: Map[Array[Byte], Array[Byte]], lastIndex: Long = 0L): F[Unit] =
    for
      _ <- trace"Restoring state machine from snapshot with ${snapshot.size} entries"
      
      // Clear existing data
      _ <- clearAllData()
      
      // Write snapshot data in batch
      writeBatch <- Sync[F].delay(new WriteBatch())
      _ <- Sync[F].delay {
        snapshot.foreach { case (key, value) =>
          writeBatch.put(kvDataCF, key, value)
        }
        
        // Update applied index
        val indexBuffer = ByteBuffer.allocate(8)
        indexBuffer.putLong(lastIndex)
        writeBatch.put(kvDataCF, APPLIED_INDEX_KEY, indexBuffer.array())
      }
      
      _ <- Sync[F].delay(db.write(writeOptions, writeBatch))
      _ <- Sync[F].delay(writeBatch.close())
      _ <- trace"Successfully restored snapshot with applied index: $lastIndex"
    yield ()

object RocksDBKVStateMachine:

  def create[F[_]: Async: Logger](
      config: RocksDBConfig
  )(using Raise[F, StateMachineError]): Resource[F, RocksDBKVStateMachine[F]] =
    
    val columnFamilyDescriptors = List(
      new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, config.createColumnFamilyOptions()),
      new ColumnFamilyDescriptor("kv_data".getBytes, config.createColumnFamilyOptions())
    )

    Resource.make {
      for
        _ <- info"Opening RocksDB KV state machine at ${config.stateMachineDbPath}/kv"
        kvDbPath = config.stateMachineDbPath.resolve("kv")
        _ <- Sync[F].delay(java.nio.file.Files.createDirectories(kvDbPath))
        
        columnFamilyHandles <- Sync[F].delay(new java.util.ArrayList[ColumnFamilyHandle]())
        db <- Sync[F].delay {
          RocksDB.open(
            config.createStateMachineDBOptions(),
            kvDbPath.toString,
            columnFamilyDescriptors.asJava,
            columnFamilyHandles
          )
        }
        
        defaultCF = columnFamilyHandles.get(0)
        kvDataCF = columnFamilyHandles.get(1)
        
        writeOptions <- Sync[F].delay {
          val opts = new WriteOptions()
          opts.setSync(true)
          opts
        }
        
        readOptions <- Sync[F].delay(new ReadOptions())
        
        stateMachine = new RocksDBKVStateMachine[F](db, kvDataCF, writeOptions, readOptions)
        _ <- info"RocksDB KV state machine opened successfully"
      yield stateMachine
    } { stateMachine =>
      for
        _ <- info"Closing RocksDB KV state machine"
        _ <- Sync[F].delay(stateMachine.readOptions.close())
        _ <- Sync[F].delay(stateMachine.writeOptions.close())
        _ <- Sync[F].delay(stateMachine.kvDataCF.close())
        _ <- Sync[F].delay(stateMachine.db.close())
        _ <- info"RocksDB KV state machine closed"
      yield ()
    }

extension [T](array: Array[T])
  def startsWith(prefix: Array[T]): Boolean =
    if prefix.length > array.length then false
    else prefix.zip(array).forall { case (p, a) => p == a }