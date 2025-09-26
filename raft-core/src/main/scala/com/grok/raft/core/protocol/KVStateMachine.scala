package com.grok.raft.core.protocol

import cats.*
import cats.mtl.Raise
import com.grok.raft.core.internal.StateMachine
import com.grok.raft.core.error.StateMachineError

/**
 * Key-Value specific state machine trait that extends the base StateMachine.
 * Provides a pluggable interface for different KV storage implementations.
 * 
 * This trait defines the contract for a distributed key-value store that:
 * - Stores keys and values as byte arrays for maximum flexibility
 * - Supports basic CRUD operations (Put, Get, Delete, Contains)
 * - Provides range queries and prefix scanning capabilities
 * - Maintains ordering for efficient range operations
 * 
 * Implementations can use different underlying storage mechanisms:
 * - In-memory TreeMap for testing
 * - RocksDB for production
 * - Custom storage engines
 * 
 * @tparam F The effect type for state machine operations
 */
trait KVStateMachine[F[_]](implicit monadF: Monad[F], raiseF: Raise[F, StateMachineError]) extends StateMachine[F, Map[Array[Byte], Array[Byte]]] {
  
  /**
   * Core KV operations that implementations must provide
   */
  def put(key: Array[Byte], value: Array[Byte]): F[Unit]
  def get(key: Array[Byte]): F[Option[Array[Byte]]]
  def delete(key: Array[Byte]): F[Unit]
  def contains(key: Array[Byte]): F[Boolean]
  
  /**
   * Range and scan operations for advanced querying
   */
  def range(startKey: Array[Byte], endKey: Array[Byte], limit: Option[Int] = None): F[List[(Array[Byte], Array[Byte])]]
  def scan(prefix: Array[Byte], limit: Option[Int] = None): F[List[(Array[Byte], Array[Byte])]]
  def keys(prefix: Option[Array[Byte]] = None, limit: Option[Int] = None): F[List[Array[Byte]]]
  
  /**
   * Default implementation of applyWrite using the KV operations above
   * Includes error handling for unknown commands
   */
  override def applyWrite: PartialFunction[(Long, WriteCommand[?]), F[Any]] = {
    case (_, Put(key, value)) => 
      if (key.isEmpty) operationFailed("Put", "empty key not allowed")
      else put(key, value).asInstanceOf[F[Any]]
    case (_, Delete(key)) => 
      if (key.isEmpty) operationFailed("Delete", "empty key not allowed")
      else delete(key).asInstanceOf[F[Any]]
  }
  
  /**
   * Default implementation of applyRead using the KV operations above
   * Includes error handling for unknown commands
   */
  override def applyRead: PartialFunction[ReadCommand[?], F[Any]] = {
    case Get(key) => 
      if (key.isEmpty) operationFailed("Get", "empty key not allowed")
      else get(key).asInstanceOf[F[Any]]
    case Contains(key) => 
      if (key.isEmpty) operationFailed("Contains", "empty key not allowed")
      else contains(key).asInstanceOf[F[Any]]
    case Range(startKey, endKey, limit) => 
      if (startKey.isEmpty || endKey.isEmpty) operationFailed("Range", "empty keys not allowed")
      else if (limit.exists(_ < 0)) operationFailed("Range", "negative limit not allowed")
      else range(startKey, endKey, limit).asInstanceOf[F[Any]]
    case Scan(prefix, limit) => 
      if (prefix.isEmpty) operationFailed("Scan", "empty prefix not allowed")
      else if (limit.exists(_ < 0)) operationFailed("Scan", "negative limit not allowed")
      else scan(prefix, limit).asInstanceOf[F[Any]]
    case Keys(prefix, limit) => 
      if (limit.exists(_ < 0)) operationFailed("Keys", "negative limit not allowed")
      else keys(prefix, limit).asInstanceOf[F[Any]]
  }
}