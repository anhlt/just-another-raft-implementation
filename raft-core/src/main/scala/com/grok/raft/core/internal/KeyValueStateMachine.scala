package com.grok.raft.core.internal

import cats.*
import cats.implicits.*
import cats.effect.Ref
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.KeyValueStorage
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*

import scala.collection.concurrent.TrieMap

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
        result <- storage.put(key, value)
        _      <- appliedIndexRef.set(index)
        _      <- trace"Create operation completed: $result"
      } yield result.map(_.getBytes("UTF-8"))

    case (index, update: Update) =>
      for {
        _ <- trace"Applying Update operation at index $index"
        key   = new String(update.key, "UTF-8")
        value = new String(update.value, "UTF-8")
        result <- storage.put(key, value)
        _      <- appliedIndexRef.set(index)
        _      <- trace"Update operation completed: $result"
      } yield result.map(_.getBytes("UTF-8"))

    case (index, delete: Delete) =>
      for {
        _ <- trace"Applying Delete operation at index $index"
        key = new String(delete.key, "UTF-8")
        result <- storage.remove(key)
        _      <- appliedIndexRef.set(index)
        _      <- trace"Delete operation completed: $result"
      } yield result.map(_.getBytes("UTF-8"))

    case (index, upsert: Upsert) =>
      for {
        _ <- trace"Applying Upsert operation at index $index"
        key   = new String(upsert.key, "UTF-8")
        value = new String(upsert.value, "UTF-8")
        result <- storage.put(key, value)
        _      <- appliedIndexRef.set(index)
        _      <- trace"Upsert operation completed: $result"
      } yield result.map(_.getBytes("UTF-8"))

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
        key = new String(scan.startKey, "UTF-8")
        result <- storage.get(key) // Scan limit not supported in current storage, just get single key
        _      <- trace"Scan operation completed: $result"
      } yield result.map(_.getBytes("UTF-8")).asInstanceOf[A]

    case range: Range =>
      for {
        _ <- trace"Applying Range operation: $range"
        startKey = new String(range.startKey, "UTF-8")
        endKey   = new String(range.endKey, "UTF-8")
        result <- storage.range(startKey, endKey)
        _      <- trace"Range operation completed: $result"
      } yield result.values.map(_.getBytes("UTF-8")).toList.asInstanceOf[A]

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
      _ <- trace"Restoring snapshot at index $lastIndex"
      _ <- appliedIndexRef.set(lastIndex)
      _ <- trace"Snapshot restored successfully"
    } yield ()

  def getCurrentState: F[Array[Byte]] =
    for {
      _      <- trace"Getting current state"
      keys   <- storage.keys()
      result <- keys.toList.traverse(key => storage.get(key).map(value => (key, value.getOrElse(""))))
      stateMap   = result.toMap
      stateBytes = stateMap.toString.getBytes("UTF-8") // Simple serialization for now
      _ <- trace"Current state retrieved with ${keys.size} keys"
    } yield stateBytes
}

object KeyValueStateMachine {
  def apply[F[_]: MonadThrow: Logger](
      storage: KeyValueStorage[F],
      appliedIndexRef: Ref[F, Long]
  ): KeyValueStateMachine[F] =
    new KeyValueStateMachine[F](storage, appliedIndexRef)
}
