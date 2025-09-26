package com.grok.raft.core.error

import com.grok.raft.core.internal.NodeAddress
import com.grok.raft.core.protocol.Command
import cats.mtl.Raise

// Domain errors that don't extend Exception - pure domain types
enum RaftError:
  case InvalidTerm(currentTerm: Long, requestTerm: Long)
  case LeadershipConflict(currentLeader: Option[NodeAddress], claimedLeader: NodeAddress)
  case ElectionTimeout
  case StepDownRequired(higherTerm: Long)
  case NotLeader(actualLeader: Option[NodeAddress])

enum LogError:
  case EntryNotFound(index: Long)
  case IndexOutOfBounds(index: Long, logLength: Long)
  case TermMismatch(expected: Long, actual: Long)
  case CommitIndexLagging(commitIndex: Long, lastIndex: Long)
  case SnapshotInstallationFailed(reason: String)
  case ConsistencyCheckFailed(prevIndex: Long, prevTerm: Long)

enum StateMachineError:
  case InvalidCommand(command: String)
  case OperationFailed(operation: String, reason: String)
  case StateCorruption(details: String)
  case ApplyCommandFailed(index: Long, command: Command)

enum MembershipError:
  case NodeNotFound(address: NodeAddress)
  case InvalidConfiguration(reason: String)
  case QuorumNotReached(required: Int, actual: Int)

// Extension methods for raising domain errors with MTL
extension (error: RaftError)
  def raise[F[_], A](using Raise[F, RaftError]): F[A] = Raise[F, RaftError].raise(error)

extension (error: LogError)
  def raise[F[_], A](using Raise[F, LogError]): F[A] = Raise[F, LogError].raise(error)

extension (error: StateMachineError)
  def raise[F[_], A](using Raise[F, StateMachineError]): F[A] = Raise[F, StateMachineError].raise(error)

extension (error: MembershipError)
  def raise[F[_], A](using Raise[F, MembershipError]): F[A] = Raise[F, MembershipError].raise(error)
