package com.grok.raft.core.internal

import cats.*
import cats.implicits.*
import cats.effect.Ref
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.KeyValueStorage
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*

import scala.collection.concurrent.TrieMap

/** Key-Value State Machine implementation that handles WriteOp and ReadOp operations using a generic KeyValueStorage as
  * the underlying storage engine.
  *
  * This implementation provides:
  *   - CRUD operations via WriteOp (Create, Update, Delete, Upsert)
  *   - Query operations via ReadOp (Get, Scan, Range, Keys)
  *   - Persistent storage backed by KeyValueStorage implementation
  *   - Thread-safe concurrent access
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
) extends StateMachine[F, Map[String, String]] {

  // In-memory cache for faster reads (optional optimization)
  private val cache = TrieMap[String, String]()

  def applyWrite: PartialFunction[(Long, WriteCommand[?]), F[Any]] = {
    case (index, writeOp: WriteOp[String, String]) =>
      for {
        _      <- trace"Applying write operation at index $index: $writeOp"
        result <- executeWriteOp(writeOp)
        _      <- appliedIndexRef.set(index)
        _      <- trace"Write operation completed: $result"
      } yield result

    case (index, otherWrite: WriteCommand[?]) =>
      for {
        _ <- trace"Unknown write command at index $index: $otherWrite"
        _ <- appliedIndexRef.set(index)
      } yield ()
  }

  def applyRead: PartialFunction[ReadCommand[?], F[Any]] = {
    case readOp: ReadOp[String, String] =>
      for {
        _      <- trace"Applying read operation: $readOp"
        result <- executeReadOp(readOp)
        _      <- trace"Read operation completed: $result"
      } yield result

    case otherRead: ReadCommand[?] =>
      for {
        _ <- trace"Unknown read command: $otherRead"
      } yield None
  }

  def appliedIndex: F[Long] =
    appliedIndexRef.get

  def restoreSnapshot[T](lastIndex: Long, data: T): F[Unit] =
    data match {
      case kvMap: Map[String, String] @unchecked =>
        for {
          _ <- trace"Restoring snapshot at index $lastIndex with ${kvMap.size} entries"
          // Clear existing data
          existingKeys <- storage.keys()
          _            <- existingKeys.toList.traverse_(storage.remove)
          _            <- MonadThrow[F].catchNonFatal(cache.clear())

          // Restore data
          _ <- kvMap.toList.traverse_ { case (k, v) =>
            for {
              _ <- storage.put(k, v)
              _ <- MonadThrow[F].catchNonFatal(cache.put(k, v))
            } yield ()
          }
          _ <- appliedIndexRef.set(lastIndex)
          _ <- trace"Snapshot restored successfully"
        } yield ()

      case _ =>
        for {
          _ <- trace"Invalid snapshot data type for KeyValueStateMachine"
          _ <- MonadThrow[F].raiseError(new IllegalArgumentException("Invalid snapshot data"))
        } yield ()
    }

  def getCurrentState: F[Map[String, String]] =
    for {
      _       <- trace"Getting current state from storage"
      allKeys <- storage.keys()
      kvPairs <- allKeys.toList.traverse { key =>
        storage.get(key).map(_.map(key -> _))
      }
      result = kvPairs.flatten.toMap
      _ <- trace"Retrieved ${result.size} entries from storage"
    } yield result

  private def executeWriteOp(writeOp: WriteOp[String, String]): F[Option[String]] =
    writeOp match {
      case Create(key, value) =>
        for {
          _        <- trace"Create operation: $key -> $value"
          existing <- storage.get(key)
          result <-
            if (existing.isDefined) {
              MonadThrow[F].pure(existing) // Return existing value, don't overwrite
            } else {
              for {
                _ <- storage.put(key, value)
                _ <- MonadThrow[F].catchNonFatal(cache.put(key, value))
              } yield Some(value) // Return the created value
            }
        } yield result

      case Update(key, value) =>
        for {
          _        <- trace"Update operation: $key -> $value"
          existing <- storage.get(key)
          result <-
            if (existing.isDefined) {
              for {
                _ <- storage.put(key, value)
                _ <- MonadThrow[F].catchNonFatal(cache.put(key, value))
              } yield Some(value) // Return the new value
            } else {
              MonadThrow[F].pure(None) // Key didn't exist
            }
        } yield result

      case Delete(key) =>
        for {
          _        <- trace"Delete operation: $key"
          existing <- storage.get(key)
          result <-
            if (existing.isDefined) {
              for {
                _ <- storage.remove(key)
                _ <- MonadThrow[F].catchNonFatal(cache.remove(key))
              } yield existing // Return deleted value
            } else {
              MonadThrow[F].pure(None)
            }
        } yield result

      case Upsert(key, value) =>
        for {
          _ <- trace"Upsert operation: $key -> $value"
          _ <- storage.put(key, value)
          _ <- MonadThrow[F].catchNonFatal(cache.put(key, value))
        } yield Some(value) // Return the new value that was set
    }

  private def executeReadOp(readOp: ReadOp[String, String]): F[Option[String]] =
    readOp match {
      case Get(key) =>
        for {
          _ <- trace"Get operation: $key"
          // Try cache first, then storage
          result <- MonadThrow[F].catchNonFatal(cache.get(key)) flatMap {
            case Some(cachedValue) => MonadThrow[F].pure(Some(cachedValue))
            case None =>
              storage.get(key).flatMap {
                case Some(value) =>
                  for {
                    _ <- MonadThrow[F].catchNonFatal(cache.put(key, value)) // Update cache
                  } yield Some(value)
                case None => MonadThrow[F].pure(None)
              }
          }
        } yield result

      case Scan(startKey, limit) =>
        for {
          _          <- trace"Scan operation: startKey=$startKey, limit=$limit"
          scanResult <- storage.scan(startKey)
          limitedResults = scanResult.take(limit)
          _ <- limitedResults.toList.traverse_ { case (k, v) =>
            MonadThrow[F].catchNonFatal(cache.put(k, v)) // Update cache
          }
          result =
            if (limitedResults.nonEmpty) {
              Some(limitedResults.map { case (k, v) => s"$k:$v" }.mkString(","))
            } else None
        } yield result

      case Range(startKey, endKey) =>
        for {
          _           <- trace"Range operation: startKey=$startKey, endKey=$endKey"
          rangeResult <- storage.range(startKey, endKey)
          _ <- rangeResult.toList.traverse_ { case (k, v) =>
            MonadThrow[F].catchNonFatal(cache.put(k, v)) // Update cache
          }
          result =
            if (rangeResult.nonEmpty) {
              Some(rangeResult.map { case (k, v) => s"$k:$v" }.mkString(","))
            } else None
        } yield result

      case Keys(prefixOpt) =>
        for {
          _ <- trace"Keys operation: prefix=$prefixOpt"
          result <- prefixOpt match {
            case Some(prefix) =>
              for {
                prefixKeys <- storage.scan(prefix).map(_.keySet)
                filteredKeys = prefixKeys.filter(_.startsWith(prefix))
              } yield if (filteredKeys.nonEmpty) Some(filteredKeys.mkString(",")) else None

            case None =>
              for {
                allKeys <- storage.keys()
              } yield if (allKeys.nonEmpty) Some(allKeys.mkString(",")) else None
          }
        } yield result
    }
}

object KeyValueStateMachine {
  def apply[F[_]: MonadThrow: Logger](
      storage: KeyValueStorage[F],
      appliedIndexRef: Ref[F, Long]
  ): KeyValueStateMachine[F] =
    new KeyValueStateMachine[F](storage, appliedIndexRef)
}
