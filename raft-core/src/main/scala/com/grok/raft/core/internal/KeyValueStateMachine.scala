package com.grok.raft.core.internal

import cats.*
import cats.implicits.*
import cats.effect.Ref
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.{KeyValueStorage, TypedSerializer}
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
  *   - Type-aware operations with typed keys and values
  *
  * @param storage
  *   The key-value storage for persistent data
  * @param appliedIndexRef
  *   Reference to track the last applied log index
  * @param keySerializer
  *   Serializer for keys of type K
  * @param valueSerializer
  *   Serializer for values of type V
  * @tparam F
  *   The effect type (e.g., IO, Task)
  * @tparam K
  *   The key type
  * @tparam V
  *   The value type
  */
class KeyValueStateMachine[F[_]: MonadThrow: Logger, K, V](
    storage: KeyValueStorage[F],
    appliedIndexRef: Ref[F, Long],
    keySerializer: TypedSerializer[K],
    valueSerializer: TypedSerializer[V]
) extends StateMachine[F, K, V] {

  // In-memory cache for faster reads (optional optimization)
  private val cache = TrieMap[String, String]()

  def applyWrite: PartialFunction[(Long, WriteCommand[K, V, Option[V]]), F[Option[V]]] = {
    case (index, create: Create[K, V]) =>
      for {
        _ <- trace"Applying Create operation at index $index"
        keyBytes   = keySerializer.serialize(create.key.value)
        valueBytes = valueSerializer.serialize(create.value.value)
        key        = new String(keyBytes, "UTF-8")
        value      = new String(valueBytes, "UTF-8")
        existing <- storage.get(key)
        result <-
          if (existing.isDefined) {
            // Key already exists, return existing value without overwriting
            MonadThrow[F].pure(existing.map(v => valueSerializer.deserialize(v.getBytes("UTF-8"))))
          } else {
            // Key doesn't exist, create it and return the new value
            for {
              _ <- storage.put(key, value)
              _ <- MonadThrow[F].catchNonFatal(cache.put(key, value))
            } yield Some(create.value.value)
          }
        _ <- appliedIndexRef.set(index)
        _ <- trace"Create operation completed: $result"
      } yield result

    case (index, update: Update[K, V]) =>
      for {
        _ <- trace"Applying Update operation at index $index"
        keyBytes   = keySerializer.serialize(update.key.value)
        valueBytes = valueSerializer.serialize(update.value.value)
        key        = new String(keyBytes, "UTF-8")
        value      = new String(valueBytes, "UTF-8")
        existing <- storage.get(key)
        result <-
          if (existing.isDefined) {
            // Key exists, update it and return the new value
            for {
              _ <- storage.put(key, value)
              _ <- MonadThrow[F].catchNonFatal(cache.put(key, value))
            } yield Some(update.value.value)
          } else {
            // Key doesn't exist, can't update
            MonadThrow[F].pure(None)
          }
        _ <- appliedIndexRef.set(index)
        _ <- trace"Update operation completed: $result"
      } yield result

    case (index, delete: Delete[K, V]) =>
      for {
        _ <- trace"Applying Delete operation at index $index"
        keyBytes = keySerializer.serialize(delete.key.value)
        key      = new String(keyBytes, "UTF-8")
        existing <- storage.get(key)
        result <-
          if (existing.isDefined) {
            // Key exists, delete it and return the deleted value
            for {
              _ <- storage.remove(key)
              _ <- MonadThrow[F].catchNonFatal(cache.remove(key))
            } yield existing.map(v => valueSerializer.deserialize(v.getBytes("UTF-8")))
          } else {
            // Key doesn't exist
            MonadThrow[F].pure(None)
          }
        _ <- appliedIndexRef.set(index)
        _ <- trace"Delete operation completed: $result"
      } yield result

    case (index, upsert: Upsert[K, V]) =>
      for {
        _ <- trace"Applying Upsert operation at index $index"
        keyBytes   = keySerializer.serialize(upsert.key.value)
        valueBytes = valueSerializer.serialize(upsert.value.value)
        key        = new String(keyBytes, "UTF-8")
        value      = new String(valueBytes, "UTF-8")
        _ <- storage.put(key, value)
        _ <- MonadThrow[F].catchNonFatal(cache.put(key, value))
        _ <- appliedIndexRef.set(index)
        _ <- trace"Upsert operation completed: returning new value"
      } yield Some(upsert.value.value)

    case (index, otherWrite: WriteCommand[K, V, Option[V]]) =>
      for {
        _ <- trace"Unknown write command at index $index: $otherWrite"
        _ <- appliedIndexRef.set(index)
      } yield None
  }

  def applyRead[A]: PartialFunction[ReadCommand[K, V, A], F[A]] = {
    case get: Get[K, V] =>
      for {
        _ <- trace"Applying Get operation: $get"
        keyBytes = keySerializer.serialize(get.key.value)
        key      = new String(keyBytes, "UTF-8")
        result <- storage.get(key)
        _      <- trace"Get operation completed: $result"
      } yield result.map(v => valueSerializer.deserialize(v.getBytes("UTF-8"))).asInstanceOf[A]

    case scan: Scan[K, V] =>
      for {
        _ <- trace"Applying Scan operation: $scan"
        keyBytes = keySerializer.serialize(scan.startKey.value)
        startKey = new String(keyBytes, "UTF-8")
        scanResult <- storage.scan(startKey)
        limited = scanResult.take(scan.limit)
        // Convert to V list
        results = limited.map { case (_, v) =>
          valueSerializer.deserialize(v.getBytes("UTF-8"))
        }.toList
        _ <- trace"Scan operation completed: ${results.size} items"
      } yield results.asInstanceOf[A]

    case range: Range[K, V] =>
      for {
        _ <- trace"Applying Range operation: $range"
        startKeyBytes = keySerializer.serialize(range.startKey.value)
        endKeyBytes   = keySerializer.serialize(range.endKey.value)
        startKey      = new String(startKeyBytes, "UTF-8")
        endKey        = new String(endKeyBytes, "UTF-8")
        result <- storage.range(startKey, endKey)
        // Convert to V list
        results = result.map { case (_, v) =>
          valueSerializer.deserialize(v.getBytes("UTF-8"))
        }.toList
        _ <- trace"Range operation completed: $results"
      } yield results.asInstanceOf[A]

    case keys: Keys[K, V] =>
      for {
        _      <- trace"Applying Keys operation: $keys"
        result <- storage.keys()
        filtered: Set[String] = keys.prefix match {
          case Some(prefixValue) =>
            val prefixBytes = keySerializer.serialize(prefixValue.value)
            val prefix      = new String(prefixBytes, "UTF-8")
            result.filter(_.startsWith(prefix))
          case None => result
        }
        // Convert to K list
        keyList = filtered.map(k => keySerializer.deserialize(k.getBytes("UTF-8"))).toList
        _ <- trace"Keys operation completed: $keyList"
      } yield keyList.asInstanceOf[A]

    case otherRead: ReadCommand[K, V, A] =>
      for {
        _ <- trace"Unknown read command: $otherRead"
      } yield None.asInstanceOf[A]
  }

  def appliedIndex: F[Long] =
    appliedIndexRef.get

  def restoreSnapshot(lastIndex: Long, data: Array[Byte]): F[Unit] =
    for {
      _             <- trace"Restoring snapshot at index $lastIndex"
      keyValuePairs <- deserializeState(data)
      _             <- trace"Deserialized ${keyValuePairs.size} key-value pairs from snapshot"

      // Clear existing state to avoid stale data
      existingKeys <- storage.keys()
      _            <- trace"Clearing ${existingKeys.size} existing keys"
      _            <- existingKeys.toList.traverse_(storage.remove)
      _            <- MonadThrow[F].catchNonFatal(cache.clear())

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
      keyValuePairs <- keys.toList.traverse(key => storage.get(key).map(value => (key, value.getOrElse(""))))
      serializedData = serializeState(keyValuePairs)
      _ <- trace"Current state serialized with ${keyValuePairs.size} entries"
    } yield serializedData

  private def serializeState(keyValuePairs: List[(String, String)]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(calculateSerializedSize(keyValuePairs))
    buffer.order(ByteOrder.BIG_ENDIAN)

    // Write number of entries
    buffer.putInt(keyValuePairs.size)

    // Write each key-value pair
    keyValuePairs.foreach { case (key, value) =>
      val keyBytes   = key.getBytes("UTF-8")
      val valueBytes = value.getBytes("UTF-8")

      buffer.putInt(keyBytes.length)
      buffer.put(keyBytes)
      buffer.putInt(valueBytes.length)
      buffer.put(valueBytes)
    }

    buffer.array()
  }

  private def deserializeState(data: Array[Byte]): F[List[(String, String)]] = {
    MonadThrow[F]
      .catchNonFatal {
        val buffer = ByteBuffer.wrap(data)
        buffer.order(ByteOrder.BIG_ENDIAN)

        val numEntries = buffer.getInt()
        val entries = (0 until numEntries).map { _ =>
          val keyLength = buffer.getInt()
          val keyBytes  = new Array[Byte](keyLength)
          buffer.get(keyBytes)
          val key = new String(keyBytes, "UTF-8")

          val valueLength = buffer.getInt()
          val valueBytes  = new Array[Byte](valueLength)
          buffer.get(valueBytes)
          val value = new String(valueBytes, "UTF-8")

          (key, value)
        }.toList

        entries
      }
      .handleErrorWith { error =>
        for {
          _ <- trace"Failed to deserialize snapshot data: ${error.getMessage}"
          _ <- MonadThrow[F].raiseError(
            new IllegalArgumentException(s"Invalid snapshot data format: ${error.getMessage}")
          )
        } yield List.empty
      }
  }

  private def calculateSerializedSize(keyValuePairs: List[(String, String)]): Int = {
    4 + // number of entries
      keyValuePairs.map { case (key, value) =>
        4 + key.getBytes("UTF-8").length +   // key length + key bytes
          4 + value.getBytes("UTF-8").length // value length + value bytes
      }.sum
  }
}

object KeyValueStateMachine {
  def apply[F[_]: MonadThrow: Logger, K, V](
      storage: KeyValueStorage[F],
      appliedIndexRef: Ref[F, Long],
      keySerializer: TypedSerializer[K],
      valueSerializer: TypedSerializer[V]
  ): KeyValueStateMachine[F, K, V] =
    new KeyValueStateMachine[F, K, V](storage, appliedIndexRef, keySerializer, valueSerializer)
}
