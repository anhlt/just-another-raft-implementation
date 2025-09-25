package com.grok.raft.core.internal

import cats.*
import cats.implicits.*
import cats.effect.Ref
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.KeyValueStorage
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*

import scala.collection.concurrent.TrieMap
import java.nio.{ByteBuffer, ByteOrder}

/** Key-Value State Machine implementation that handles Write and Read operations using a generic KeyValueStorage as the
  * underlying storage engine.
  *
  * This implementation provides:
  *   - CRUD operations via WriteCommand (Create, Update, Delete, Upsert)
  *   - Query operations via ReadCommand (Get, Scan, Range, Keys)
  *   - Persistent storage backed by KeyValueStorage implementation
  *   - Thread-safe concurrent access
  *   - Works with raw bytes for keys and values to eliminate type parameter complexity
  *
  * @param storage
  *   The key-value storage for persistent data
  * @param appliedIndexRef
  *   Reference to track the last applied log index
  * @tparam F
  *   The effect type (e.g., IO, Task)
  */
class KeyValueStateMachine[F[_]: MonadThrow: Logger](
    storage: KeyValueStorage[F],
    appliedIndexRef: Ref[F, Long]
) extends StateMachine[F] {

  // In-memory cache for faster reads (optional optimization)
  private val cache = TrieMap[String, String]()

  def applyWrite: PartialFunction[(Long, WriteCommand[Option[Array[Byte]]]), F[Option[Array[Byte]]]] = {
    case (index, create: Create) =>
      for {
        _ <- trace"Applying Create operation at index $index"
        key   = new String(create.key, "UTF-8")
        value = new String(create.value, "UTF-8")
        existing <- storage.get(key)
        result <- 
          if (existing.isDefined) {
            // Key already exists, return existing value without overwriting
            MonadThrow[F].pure(existing)
          } else {
            // Key doesn't exist, create it and return the new value
            for {
              _ <- storage.put(key, value)
              _ <- MonadThrow[F].catchNonFatal(cache.put(key, value))
            } yield Some(value)
          }
        _      <- appliedIndexRef.set(index)
        _      <- trace"Create operation completed: $result"
      } yield result.map(_.getBytes("UTF-8"))

    case (index, update: Update) =>
      for {
        _ <- trace"Applying Update operation at index $index"
        key   = new String(update.key, "UTF-8")
        value = new String(update.value, "UTF-8")
        existing <- storage.get(key)
        result <- 
          if (existing.isDefined) {
            // Key exists, update it and return the new value
            for {
              _ <- storage.put(key, value)
              _ <- MonadThrow[F].catchNonFatal(cache.put(key, value))
            } yield Some(value)
          } else {
            // Key doesn't exist, can't update
            MonadThrow[F].pure(None)
          }
        _      <- appliedIndexRef.set(index)
        _      <- trace"Update operation completed: $result"
      } yield result.map(_.getBytes("UTF-8"))

    case (index, delete: Delete) =>
      for {
        _ <- trace"Applying Delete operation at index $index"
        key = new String(delete.key, "UTF-8")
        existing <- storage.get(key)
        result <- 
          if (existing.isDefined) {
            // Key exists, delete it and return the deleted value
            for {
              _ <- storage.remove(key)
              _ <- MonadThrow[F].catchNonFatal(cache.remove(key))
            } yield existing
          } else {
            // Key doesn't exist
            MonadThrow[F].pure(None)
          }
        _      <- appliedIndexRef.set(index)
        _      <- trace"Delete operation completed: $result"
      } yield result.map(_.getBytes("UTF-8"))

    case (index, upsert: Upsert) =>
      for {
        _ <- trace"Applying Upsert operation at index $index"
        key   = new String(upsert.key, "UTF-8")
        value = new String(upsert.value, "UTF-8")
        _ <- storage.put(key, value)
        _ <- MonadThrow[F].catchNonFatal(cache.put(key, value))
        _      <- appliedIndexRef.set(index)
        _      <- trace"Upsert operation completed: returning new value"
      } yield Some(value.getBytes("UTF-8"))

    case (index, otherWrite: WriteCommand[Option[Array[Byte]]]) =>
      for {
        _ <- trace"Unknown write command at index $index: $otherWrite"
        _ <- appliedIndexRef.set(index)
      } yield None
  }

  def applyRead[A]: PartialFunction[ReadCommand[A], F[A]] = {
    case get: Get =>
      for {
        _ <- trace"Applying Get operation: $get"
        key = new String(get.key, "UTF-8")
        result <- storage.get(key)
        _      <- trace"Get operation completed: $result"
      } yield result.map(_.getBytes("UTF-8")).asInstanceOf[A]

    case scan: Scan =>
      for {
        _ <- trace"Applying Scan operation: $scan"
        startKey = new String(scan.startKey, "UTF-8")
        scanResult <- storage.scan(startKey)
        limited = scanResult.take(scan.limit)
        // Format as key:value pairs, each as a separate element
        formattedResult = limited.map { case (k, v) => s"$k:$v".getBytes("UTF-8") }
        _      <- trace"Scan operation completed: ${formattedResult.size} items"
      } yield formattedResult.asInstanceOf[A]

    case range: Range =>
      for {
        _ <- trace"Applying Range operation: $range"
        startKey = new String(range.startKey, "UTF-8")
        endKey   = new String(range.endKey, "UTF-8")
        result <- storage.range(startKey, endKey)
        // Format as key:value pairs
        formattedPairs = result.map { case (k, v) => s"$k:$v" }.toList
        _      <- trace"Range operation completed: $formattedPairs"
      } yield formattedPairs.map(_.getBytes("UTF-8")).asInstanceOf[A]

    case keys: Keys =>
      for {
        _      <- trace"Applying Keys operation: $keys"
        result <- storage.keys()
        filtered = keys.prefix match {
          case Some(prefixBytes) =>
            val prefix = new String(prefixBytes, "UTF-8")
            result.filter(_.startsWith(prefix))
          case None => result
        }
        _ <- trace"Keys operation completed: $filtered"
      } yield filtered.map(_.getBytes("UTF-8")).toList.asInstanceOf[A]

    case otherRead: ReadCommand[A] =>
      for {
        _ <- trace"Unknown read command: $otherRead"
      } yield None.asInstanceOf[A]
  }

  def appliedIndex: F[Long] =
    appliedIndexRef.get

  def restoreSnapshot(lastIndex: Long, data: Array[Byte]): F[Unit] =
    for {
      _              <- trace"Restoring snapshot at index $lastIndex"
      keyValuePairs  <- deserializeState(data)
      _              <- trace"Deserialized ${keyValuePairs.size} key-value pairs from snapshot"
      
      // Clear existing state to avoid stale data
      existingKeys   <- storage.keys()
      _              <- trace"Clearing ${existingKeys.size} existing keys"
      _              <- existingKeys.toList.traverse_(storage.remove)
      _              <- MonadThrow[F].catchNonFatal(cache.clear())
      
      // Restore state from snapshot
      _ <- keyValuePairs.traverse_ { case (k, v) =>
        for {
          _ <- storage.put(k, v)
          _ <- MonadThrow[F].catchNonFatal(cache.put(k, v))
        } yield ()
      }
      
      // Update applied index
      _ <- appliedIndexRef.set(lastIndex)
      _ <- trace"Snapshot restored successfully with ${keyValuePairs.size} entries at index $lastIndex"
    } yield ()

  def getCurrentState: F[Array[Byte]] =
    for {
      _             <- trace"Getting current state"
      keys          <- storage.keys()
      keyValuePairs <- keys.toList.traverse(key => 
        storage.get(key).map(value => (key, value.getOrElse("")))
      )
      serializedData = serializeState(keyValuePairs)
      _             <- trace"Current state serialized with ${keyValuePairs.size} entries"
    } yield serializedData

  private def serializeState(keyValuePairs: List[(String, String)]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(calculateSerializedSize(keyValuePairs))
    buffer.order(ByteOrder.BIG_ENDIAN)
    
    // Write number of entries
    buffer.putInt(keyValuePairs.size)
    
    // Write each key-value pair
    keyValuePairs.foreach { case (key, value) =>
      val keyBytes = key.getBytes("UTF-8")
      val valueBytes = value.getBytes("UTF-8")
      
      buffer.putInt(keyBytes.length)
      buffer.put(keyBytes)
      buffer.putInt(valueBytes.length)
      buffer.put(valueBytes)
    }
    
    buffer.array()
  }

  private def deserializeState(data: Array[Byte]): F[List[(String, String)]] = {
    MonadThrow[F].catchNonFatal {
      val buffer = ByteBuffer.wrap(data)
      buffer.order(ByteOrder.BIG_ENDIAN)
      
      val numEntries = buffer.getInt()
      val entries = (0 until numEntries).map { _ =>
        val keyLength = buffer.getInt()
        val keyBytes = new Array[Byte](keyLength)
        buffer.get(keyBytes)
        val key = new String(keyBytes, "UTF-8")
        
        val valueLength = buffer.getInt()
        val valueBytes = new Array[Byte](valueLength)
        buffer.get(valueBytes)
        val value = new String(valueBytes, "UTF-8")
        
        (key, value)
      }.toList
      
      entries
    }.handleErrorWith { error =>
      for {
        _ <- trace"Failed to deserialize snapshot data: ${error.getMessage}"
        _ <- MonadThrow[F].raiseError(new IllegalArgumentException(s"Invalid snapshot data format: ${error.getMessage}"))
      } yield List.empty
    }
  }

  private def calculateSerializedSize(keyValuePairs: List[(String, String)]): Int = {
    4 + // number of entries
    keyValuePairs.map { case (key, value) =>
      4 + key.getBytes("UTF-8").length + // key length + key bytes
      4 + value.getBytes("UTF-8").length  // value length + value bytes
    }.sum
  }
}

object KeyValueStateMachine {
  def apply[F[_]: MonadThrow: Logger](
      storage: KeyValueStorage[F],
      appliedIndexRef: Ref[F, Long]
  ): KeyValueStateMachine[F] =
    new KeyValueStateMachine[F](storage, appliedIndexRef)
}
