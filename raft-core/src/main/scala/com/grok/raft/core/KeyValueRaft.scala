package com.grok.raft.core

import cats.*
import cats.effect.*
import cats.implicits.*
import com.grok.raft.core.internal.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*

/** Factory for creating a Raft-based distributed key-value store.
  */
object KeyValueRaft:

  /** Creates a new KeyValue Raft instance with the provided KeyValueStorage implementation.
    */
  def create[F[_]: Async: Logger](
      storage: KeyValueStorage[F]
  ): F[KeyValueRaft[F]] =
    for {
      // Create state machine
      appliedIndexRef <- Ref.of[F, Long](0L)
      keySerializer   = TypedSerializer.forType(StringType)
      valueSerializer = TypedSerializer.forType(StringType)
      stateMachine = new KeyValueStateMachine[F, String, String](
        storage,
        appliedIndexRef,
        keySerializer,
        valueSerializer
      )

      _ <- trace"KeyValue Raft components initialized"
    } yield new KeyValueRaft[F](stateMachine, storage)

/** A simplified Raft-based distributed key-value store.
  */
class KeyValueRaft[F[_]: MonadThrow: Logger](
    private val stateMachine: KeyValueStateMachine[F, String, String],
    private val storage: KeyValueStorage[F]
):

  /** Store a key-value pair (goes through Raft consensus).
    */
  def put(key: String, value: String): F[Option[String]] =
    for {
      _      <- trace"KV Put: $key -> $value"
      result <- stateMachine.applyWrite.apply((0L, Upsert(TypedValue(key, StringType), TypedValue(value, StringType))))
      _      <- trace"KV Put completed: $result"
    } yield result

  /** Retrieve a value by key (can bypass consensus for reads).
    */
  def get(key: String, bypassConsensus: Boolean = false): F[Option[String]] =
    if (bypassConsensus) {
      // Direct read from local storage
      for {
        _      <- trace"KV Get (bypass): $key"
        result <- storage.get(key)
        _      <- trace"KV Get (bypass) completed: $result"
      } yield result
    } else {
      // Go through state machine
      for {
        _      <- trace"KV Get (consensus): $key"
        result <- stateMachine.applyRead.apply(Get(TypedValue(key, StringType)))
        _      <- trace"KV Get (consensus) completed: $result"
      } yield result
    }

  /** Remove a key-value pair (goes through Raft consensus).
    */
  def delete(key: String): F[Option[String]] =
    for {
      _      <- trace"KV Delete: $key"
      result <- stateMachine.applyWrite.apply((0L, Delete[String, String](TypedValue(key, StringType))))
      _      <- trace"KV Delete completed: $result"
    } yield result

  /** Scan multiple entries starting from a key.
    */
  def scan(startKey: String, limit: Int, bypassConsensus: Boolean = false): F[List[String]] =
    if (bypassConsensus) {
      // Direct scan from local storage
      for {
        _          <- trace"KV Scan (bypass): $startKey, limit=$limit"
        scanResult <- storage.scan(startKey)
        limitedResults = scanResult.take(limit)
        result         = limitedResults.map { case (_, v) => v }.toList
        _ <- trace"KV Scan (bypass) completed: $result"
      } yield result
    } else {
      // Go through state machine
      for {
        _      <- trace"KV Scan (consensus): $startKey, limit=$limit"
        result <- stateMachine.applyRead.apply(Scan(TypedValue(startKey, StringType), limit))
        _      <- trace"KV Scan (consensus) completed: $result"
      } yield result
    }

  /** Get a range of values between two keys.
    */
  def range(startKey: String, endKey: String, bypassConsensus: Boolean = false): F[List[String]] =
    if (bypassConsensus) {
      // Direct access from local storage
      for {
        _           <- trace"KV Range (bypass): $startKey to $endKey"
        rangeResult <- storage.range(startKey, endKey)
        result = rangeResult.values.toList
        _ <- trace"KV Range (bypass) completed: $result"
      } yield result
    } else {
      // Go through state machine
      for {
        _      <- trace"KV Range (consensus): $startKey to $endKey"
        result <- stateMachine.applyRead.apply(Range(TypedValue(startKey, StringType), TypedValue(endKey, StringType)))
        _      <- trace"KV Range (consensus) completed: $result"
      } yield result
    }

  /** Get all keys with optional prefix filtering.
    */
  def keys(prefix: Option[String] = None, bypassConsensus: Boolean = false): F[List[String]] =
    if (bypassConsensus) {
      // Direct access from local storage
      for {
        _ <- trace"KV Keys (bypass): prefix=$prefix"
        result <- prefix match {
          case Some(p) =>
            for {
              prefixScan <- storage.scan(p)
              filteredKeys = prefixScan.keySet.filter(_.startsWith(p)).toList
            } yield filteredKeys

          case None =>
            for {
              allKeys <- storage.keys()
            } yield allKeys.toList
        }
        _ <- trace"KV Keys (bypass) completed: $result"
      } yield result
    } else {
      // Go through state machine
      for {
        _      <- trace"KV Keys (consensus): prefix=$prefix"
        result <- stateMachine.applyRead.apply(Keys(prefix.map(p => TypedValue(p, StringType))))
        _      <- trace"KV Keys (consensus) completed: $result"
      } yield result
    }

  /** Stop the system and close resources.
    */
  def close(): F[Unit] =
    for {
      _ <- trace"Closing KeyValue Raft system"
      _ <- storage.close()
      _ <- trace"KeyValue Raft system closed"
    } yield ()

  /** Get the state machine for advanced operations.
    */
  def getStateMachine: KeyValueStateMachine[F, String, String] = stateMachine
