package com.grok.raft.core.internal

import cats.syntax.all.*
import cats.mtl.Raise

import com.grok.raft.core.protocol.*
import com.grok.raft.core.error.StateMachineError
import com.grok.raft.core.error.* // Import extension methods

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
  * The type parameter F[_] represents the effect type in which state machine operations are performed (e.g., IO,
  * Future, Task).
  *
  * @tparam F
  *   The effect type for state machine operations
  */
trait StateMachine[F[_], T]:
  def applyWrite: PartialFunction[(Long, WriteCommand[?]), F[Any]]
  def applyRead: PartialFunction[ReadCommand[?], F[Any]]
  def appliedIndex: F[Long]
  def restoreSnapshot[T](lastIndex: Long, data: T): F[Unit]
  def getCurrentState: F[T]

  // MTL-based error raising methods
  protected def invalidCommand[A](command: Any)(using Raise[F, StateMachineError]): F[A] =
    StateMachineError.InvalidCommand(command.getClass.getSimpleName).raise[F, A]

  protected def operationFailed[A](operation: String, reason: String)(using Raise[F, StateMachineError]): F[A] =
    StateMachineError.OperationFailed(operation, reason).raise[F, A]

  protected def corruptedState[A](details: String)(using Raise[F, StateMachineError]): F[A] =
    StateMachineError.StateCorruption(details).raise[F, A]

  protected def applyCommandFailed[A](index: Long, command: WriteCommand[?])(using Raise[F, StateMachineError]): F[A] =
    StateMachineError.ApplyCommandFailed(index, command).raise[F, A]
