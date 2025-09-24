package com.grok.raft.core

import cats.*
import cats.effect.*
import cats.implicits.*
import com.grok.raft.core.internal.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*

/**
 * Factory for creating a Raft-based distributed key-value store.
 */
object KeyValueRaft:

  /**
   * Creates a new KeyValue Raft instance with the provided KeyValueStorage implementation.
   */
  def create[F[_]: Async: Logger](
    config: ClusterConfiguration,
    storage: KeyValueStorage[F],
    membershipManager: MembershipManager[F],
    rpcClient: RpcClient[F]
  ): F[KeyValueRaft[F]] =
    for {
      // Create state machine
      appliedIndexRef <- Ref.of[F, Long](0L)
      stateMachine = new KeyValueStateMachine[F](storage, appliedIndexRef)
      
      _ <- trace"KeyValue Raft components initialized"
    } yield new KeyValueRaft[F](stateMachine, storage, config, membershipManager, rpcClient)

/**
 * A simplified Raft-based distributed key-value store.
 */
class KeyValueRaft[F[_]: MonadThrow: Logger](
  private val stateMachine: KeyValueStateMachine[F],
  private val storage: KeyValueStorage[F],
  private val config: ClusterConfiguration,
  private val membershipManager: MembershipManager[F],
  private val rpcClient: RpcClient[F]
):

  /**
   * Store a key-value pair (goes through Raft consensus).
   */
  def put(key: String, value: String): F[Option[String]] =
    for {
      _ <- trace"KV Put: $key -> $value"
      result <- stateMachine.applyWrite.apply((0L, Upsert(key, value)))
      _ <- trace"KV Put completed: $result"
    } yield result.asInstanceOf[Option[String]]

  /**
   * Retrieve a value by key (can bypass consensus for reads).
   */
  def get(key: String, bypassConsensus: Boolean = false): F[Option[String]] =
    if (bypassConsensus) {
      // Direct read from local storage
      for {
        _ <- trace"KV Get (bypass): $key"
        result <- storage.get(key)
        _ <- trace"KV Get (bypass) completed: $result"
      } yield result
    } else {
      // Go through state machine
      for {
        _ <- trace"KV Get (consensus): $key"
        result <- stateMachine.applyRead.apply(Get(key))
        _ <- trace"KV Get (consensus) completed: $result"
      } yield result.asInstanceOf[Option[String]]
    }

  /**
   * Remove a key-value pair (goes through Raft consensus).
   */
  def delete(key: String): F[Option[String]] =
    for {
      _ <- trace"KV Delete: $key"
      result <- stateMachine.applyWrite.apply((0L, Delete[String, String](key)))
      _ <- trace"KV Delete completed: $result"
    } yield result.asInstanceOf[Option[String]]

  /**
   * Scan multiple entries starting from a key.
   */
  def scan(startKey: String, limit: Int, bypassConsensus: Boolean = false): F[Option[String]] =
    if (bypassConsensus) {
      // Direct scan from local storage
      for {
        _ <- trace"KV Scan (bypass): $startKey, limit=$limit"
        scanResult <- storage.scan(startKey)
        limitedResults = scanResult.take(limit)
        result = if (limitedResults.nonEmpty) {
          Some(limitedResults.map { case (k, v) => s"$k:$v" }.mkString(","))
        } else None
        _ <- trace"KV Scan (bypass) completed: $result"
      } yield result
    } else {
      // Go through state machine
      for {
        _ <- trace"KV Scan (consensus): $startKey, limit=$limit"
        result <- stateMachine.applyRead.apply(Scan(startKey, limit))
        _ <- trace"KV Scan (consensus) completed: $result"
      } yield result.asInstanceOf[Option[String]]
    }

  /**
   * Get all keys with optional prefix filtering.
   */
  def keys(prefix: Option[String] = None, bypassConsensus: Boolean = false): F[Option[String]] =
    if (bypassConsensus) {
      // Direct access from local storage
      for {
        _ <- trace"KV Keys (bypass): prefix=$prefix"
        result <- prefix match {
          case Some(p) =>
            for {
              prefixScan <- storage.scan(p)
              filteredKeys = prefixScan.keySet.filter(_.startsWith(p))
            } yield if (filteredKeys.nonEmpty) Some(filteredKeys.mkString(",")) else None
            
          case None =>
            for {
              allKeys <- storage.keys()
            } yield if (allKeys.nonEmpty) Some(allKeys.mkString(",")) else None
        }
        _ <- trace"KV Keys (bypass) completed: $result"
      } yield result
    } else {
      // Go through state machine
      for {
        _ <- trace"KV Keys (consensus): prefix=$prefix"
        result <- stateMachine.applyRead.apply(Keys[String, String](prefix))
        _ <- trace"KV Keys (consensus) completed: $result"
      } yield result.asInstanceOf[Option[String]]
    }

  /**
   * Stop the system and close resources.
   */
  def close(): F[Unit] = 
    for {
      _ <- trace"Closing KeyValue Raft system"
      _ <- storage.close()
      _ <- trace"KeyValue Raft system closed"
    } yield ()

  /**
   * Get the state machine for advanced operations.
   */
  def getStateMachine: KeyValueStateMachine[F] = stateMachine