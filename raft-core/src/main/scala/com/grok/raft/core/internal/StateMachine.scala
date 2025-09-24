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
  * This simplified version works with raw bytes to eliminate type parameter complexity:
  *   - Write operations work with byte arrays and return Option[Array[Byte]]
  *   - Read operations can return different types:
  *     - Get/Scan operations return Option[Array[Byte]]
  *     - Range operations return List[Array[Byte]]
  *     - Keys operations return List[Array[Byte]]
  *   - Internal state is maintained as Array[Byte]
  *
  * @tparam F
  *   The effect type for state machine operations
  */
trait StateMachine[F[_]]:
  def applyWrite: PartialFunction[(Long, WriteCommand[Option[Array[Byte]]]), F[Option[Array[Byte]]]]
  def applyRead[A]: PartialFunction[ReadCommand[A], F[A]]
  def appliedIndex: F[Long]
  def restoreSnapshot(lastIndex: Long, data: Array[Byte]): F[Unit]
  def getCurrentState: F[Array[Byte]]
