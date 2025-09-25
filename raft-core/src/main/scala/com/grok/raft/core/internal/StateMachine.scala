package com.grok.raft.core.internal

import com.grok.raft.core.protocol.*

/** Represents the application state machine in a Raft distributed system.
  *
  * In the Raft consensus algorithm, each server contains a replicated log and a state machine. The state machine is the
  * component that applies committed log entries (commands) to produce the actual application state. This trait defines
  * the interface for such a state machine.
  *
  * According to the Raft paper, a state machine is typically a deterministic component that:
  *   - Receives commands from the replicated log
  *   - Applies them in sequence
  *   - Returns results to clients
  *   - Maintains the actual application state (commonly a key-value store)
  *
  * While often implemented as a key-value store, the state machine can be any application that requires distributed
  * consensus, as long as it behaves deterministically when processing the same sequence of commands.
  *
  * This type-aware version supports typed operations:
  *   - Write operations work with key-value pairs of types K, V
  *   - Read operations can return different types based on the operation
  *   - Internal state serialization is handled via Array[Byte] for snapshots
  *
  * @tparam F
  *   The effect type for state machine operations
  * @tparam K
  *   The key type for the state machine
  * @tparam V
  *   The value type for the state machine
  */
trait StateMachine[F[_], K, V]:
  def applyWrite: PartialFunction[(Long, WriteCommand[K, V, Option[V]]), F[Option[V]]]
  def applyRead[A]: PartialFunction[ReadCommand[K, V, A], F[A]]
  def appliedIndex: F[Long]
  def restoreSnapshot(lastIndex: Long, data: Array[Byte]): F[Unit]
  def getCurrentState: F[Array[Byte]]
