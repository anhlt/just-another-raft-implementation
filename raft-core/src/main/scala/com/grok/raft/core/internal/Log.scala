package com.grok.raft.core.internal

import cats.*
import cats.implicits.*
import cats.syntax.all.*
import com.grok.raft.core.*
import com.grok.raft.core.error.*
import com.grok.raft.core.internal.storage.*
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*

import scala.collection.concurrent.TrieMap

trait Log[F[_], T]:

  val logStorage: LogStorage[F]

  val snapshotStorage: SnapshotStorage[F, T]

  private val deferreds = TrieMap[Long, RaftDeferred[F, Any]]()

  val membershipManager: MembershipManager[F]

  def transactional[A](t: => F[A]): F[A]

  val stateMachine: StateMachine[F, T]


  def initialize(using
      MonadThrow[F],
      Logger[F]
  ): F[Unit] =
    for {
      snapshot <- snapshotStorage.retrieveSnapshot
      _ <- snapshot.map(restoreSnapshot).getOrElse(Monad[F].unit)
      committedIndex <- getCommittedIndex
      stateMachineIndex <- stateMachine.appliedIndex
      _ <- {
        if (stateMachineIndex > committedIndex) setCommitIndex(stateMachineIndex)
        else (stateMachineIndex to committedIndex).toList.traverse(commitLog).void
      } 
    } yield ()



  /** Methods to access and modify the commit index. The commit index represents the highest log entry known to be
    * committed in the Raft consensus.
    *
    * Implementation note: These operations should be atomic to ensure consistency in a concurrent environment.
    */

  /** Retrieves the current commit index.
    *
    * @return
    *   The current commit index wrapped in effect type F
    */
  def getCommittedIndex: F[Long]

  /** Updates the commit index to a new value.
    *
    * @param index
    *   The new commit index value (0-based)
    * @return
    *   Unit wrapped in effect type F
    */
  def setCommitIndex(index: Long): F[Unit]

  def state: F[LogState]

  def get(index: Long): F[Option[LogEntry]] =
    logStorage.get(index)

    /** Truncates log entries that are inconsistent with the leader's log to ensure safe appending of new entries. This
      * method is part of the log replication process in the Raft consensus algorithm, helping to maintain log
      * consistency by detecting and removing divergent entries. It implements the inconsistency resolution step from
      * the AppendEntries RPC.
      *
      * @param entries
      *   The list of incoming log entries from the leader, used to check for term mismatches.
      * @param leaderPrevLogIndex
      *   The index of the log entry in the leader's log immediately preceding the new entries.
      * @param currentLogIndex
      *   The current last index of the local log, to identify the last entry for comparison.
      * @return
      *   F[Unit] An effect that performs the truncation operation if an inconsistency is found, or does nothing
      *   otherwise.
      * @usecases
      *   Used during AppendEntries RPC handling (step 2 and 3 in Figure 2 of the Raft paper) to resolve log conflicts
      *   before appending new entries.
      * @see
      *   Section 5.3 and Figure 2 of the Raft paper for the consistency check and truncation logic.
      * @note
      *   This operation enforces the Log Matching Property by deleting entries after detecting term conflicts, ensuring
      *   safety in distributed consensus.
      */

  def truncateInconsistencyLog(entries: List[LogEntry], leaderPrevLogIndex: Long, currentLogIndex: Long)(using
      Monad[F],
      Logger[F]
  ): F[Unit] =
    if (entries.nonEmpty && currentLogIndex > leaderPrevLogIndex) {
      for {
        lastEntryOnCurrentNode <- logStorage.get(currentLogIndex)
        result <-
          if (lastEntryOnCurrentNode.isDefined && lastEntryOnCurrentNode.get.term != entries.head.term)
            logStorage.deleteAfter(leaderPrevLogIndex)
          else Monad[F].unit

      } yield result

    } else {
      Monad[F].unit
    }

  /** Appends new log entries to the local log after handling any necessary truncation for inconsistencies. This method
    * is a helper function in the log replication process, likely used internally within AppendEntries handling. It
    * assumes that any prior inconsistencies have been resolved (e.g., via truncateInconsistencyLog) and focuses on
    * storing the entries in the log storage. Based on Raft's log replication, this ensures that entries are added
    * atomically and in order, supporting the leader's role in maintaining a consistent replicated log.
    *
    * @param entries
    *   The list of log entries to append to the log.
    * @param leaderPrevLogIndex
    *   The index up to which the log is known to be consistent, used to position new entries correctly.
    * @param currentLogIndex
    *   The current last index of the local log, for reference in appending or updating the log state.
    * @return
    *   F[Unit] An effect that performs the append operation; it does not return a value but may handle errors or
    *   logging.
    * @usecases
    *   Invoked after inconsistency checks in AppendEntries RPC to add new entries to the log storage.
    * @note
    *   Inferred from Raft's AppendEntries mechanism (Section 5.3), where entries are appended only after confirming
    *   consistency. This function may involve transactional updates to ensure durability, as described in the paper's
    *   commitment rules.
    */
  def putEntries(entries: List[LogEntry], leaderPrevLogIndex: Long, currentLogIndex: Long)(using
      Monad[F],
      Logger[F]
  ): F[Unit] = 
    val logEntries = if (leaderPrevLogIndex + entries.size > currentLogIndex) {
      entries.drop((currentLogIndex - leaderPrevLogIndex).toInt)
    } else List.empty[LogEntry]

    logEntries.traverse { entry =>
      logStorage.put(entry.index, entry) *> trace"Entry appended: ${entry}"
    }.void
  



  /** Appends new log entries received from the leader, ensuring log consistency and handling commitment. This method
    * implements the AppendEntries RPC mechanism from the Raft consensus algorithm, which is used to replicate log
    * entries and detect/resolve inconsistencies. It performs a consistency check, truncates any conflicting entries if
    * necessary, and updates the commit index if provided by the leader.
    *
    * @param entries
    *   The list of log entries to append, each containing a command and its associated term.
    * @param leaderPrevLogIndex
    *   The index of the log entry in the leader's log immediately preceding the new entries, used for consistency
    *   checking (corresponds to prevLogIndex in Raft).
    * @param leaderCommit
    *   The leader's commit index, indicating the highest log entry known to be committed by the leader.
    * @return
    *   F[Boolean] An effect that returns true if the append operation was successful (e.g., entries were appended and
    *   no conflicts were found), false otherwise. This reflects the success condition in Raft's AppendEntries RPC.
    * @usecases
    *   Called during the AppendEntries RPC handling to synchronize logs between leader and followers.
    * @see
    *   Section 5.3 of the Raft paper for the detailed RPC mechanism, including steps for conflict detection and
    *   resolution.
    * @note
    *   This operation depends on Monad and Logger effects for asynchronous behavior and logging. It ensures the Log
    *   Matching Property by rejecting or truncating inconsistent logs.
    */
  def appendEntries(entries: List[LogEntry], leaderPrevLogIndex: Long, leaderCommit: Long)(using
      MonadThrow[F],
      Logger[F]
  ): F[Boolean] =
    transactional {
      for {
        currentLogIndex  <- logStorage.lastIndex
        appliedIndex     <- getCommittedIndex
        _                <- truncateInconsistencyLog(entries, leaderPrevLogIndex, currentLogIndex)
        _                <- putEntries(entries, leaderPrevLogIndex, currentLogIndex)
        committed        <- ((appliedIndex + 1) to leaderCommit).toList.traverse(length => commitLog(length - 1))

        _ <- if (committed.nonEmpty) compactLogs() else Monad[F].unit
      } yield committed.nonEmpty
    }

  def append[T](term: Long, command: Command, deferred: RaftDeferred[F, T])(using MonadThrow[F], Logger[F]): F[LogEntry] =
    transactional {
      for {
        lastIndex <- logStorage.lastIndex
        logEntry = LogEntry(term, lastIndex + 1, command)
        _ <- trace"Appending a command to the log. Term: ${term}, Index: ${lastIndex + 1}"
        _ <- logStorage.put(logEntry.index, logEntry)
        _ = deferreds.put(logEntry.index, deferred.asInstanceOf[RaftDeferred[F, Any]])
        _ <- trace"Entry appended. Term: ${term}, Index: ${lastIndex + 1}"
      } yield logEntry
    }

  /** Attempts to commit logs based on acknowledgment lengths from nodes in the cluster.
    *
    * This method:
    *   1. Determines the current log length 2. Identifies the current committed log length 3. For each log entry
    *      between the committed length and total length:
    *      - Verifies quorum acknowledgment
    *      - Attempts to commit entries that have reached quorum
    *
    * @param ackIndexMap
    *   A map of node addresses to their acknowledged log indices
    * @return
    *   A wrapped boolean value indicating whether any new entries were committed (true) or not (false)
    */
  def commitLogs(ackIndexMap: Map[NodeAddress, Long])(using MonadThrow[F], Logger[F]): F[Boolean] = {
    for {
      lastLogIndex   <- logStorage.lastIndex
      commitedIndex  <- getCommittedIndex
      _              <- trace"Last log index: $lastLogIndex, Committed index: $commitedIndex"
      _              <- trace"Received ackIndexMap: $ackIndexMap"
      _              <- trace"Checking for quorum"
      commited       <- ((commitedIndex + 1) to lastLogIndex).toList.traverse(index => commitIfMatch(ackIndexMap, index))
      _              <- trace"all log commited"
    } yield commited.contains(true) || commited.isEmpty
  }

  def commitIfMatch(ackedIndexMap: Map[NodeAddress, Long], index: Long)(using
      MonadThrow[F],
      Logger[F]
  ): F[Boolean] = {
    for {
      config <- membershipManager.getClusterConfiguration
      ackedCount = ackedIndexMap.count { case (_, ackedIndex) => ackedIndex >= index } // direct index comparison
      _      <- trace"ackedCount: ${ackedCount}, config: ${config.members.size}"
      result <- if (ackedCount >= config.quorumSize) commitLog(index) *> Monad[F].pure(true) else Monad[F].pure(false)
    } yield (result)
  }

  def commitLog(index: Long)(using
      MonadThrow[F],
      Logger[F]
  ): F[Unit] = {
    for {
      _ <- trace"Attempting to commit log entry at index: ${index}"
      logEntry <- logStorage.get(index)
      _        <- logEntry match {
        case Some(entry) => Monad[F].pure(entry)
        case None        => MonadThrow[F].raiseError(LogError("Log entry not found for commit."))
      }
      _        <- trace"Committing log entry: ${logEntry}"
      _        <- applyCommand(logEntry.get.index, logEntry.get.command)
      _        <- setCommitIndex(logEntry.get.index)
      _        <- trace"Log entry committed: ${logEntry}"
    } yield ()
  }

  def applyCommand(index: Long, command: Command)(using MonadThrow[F]): F[Unit] = {
    val output = command match {

      case command: ReadCommand[_] =>
        stateMachine.applyRead.apply(command)

      case command: WriteCommand[_] =>
        stateMachine.applyWrite.apply((index, command))
    }

    output.flatMap(result =>
      deferreds.get(index) match {
        case Some(deferred) => deferred.complete(result) *> Monad[F].pure(deferreds.remove(index)) *> Monad[F].unit
        case None           => Monad[F].unit
      }
    )
  }

  def applyReadCommand[T](command: ReadCommand[?])(using MonadThrow[F]): F[T] =
    stateMachine.applyRead.apply(command).asInstanceOf[F[T]]

  def compactLogs()(using MonadThrow[F], Logger[F]): F[Unit] = {
    for {
      appliedIndex <- stateMachine.appliedIndex
      shouldCompact <- shouldCreateSnapshot()
      _ <- trace"Starting log compaction up to applied index: $appliedIndex, should compact: $shouldCompact"
      _ <- if (appliedIndex > 0 && shouldCompact) {
        for {
          snapshot <- createSnapshot(appliedIndex)
          _ <- logStorage.deleteBefore(appliedIndex)
          _ <- trace"Log entries before index $appliedIndex have been compacted"
        } yield ()
      } else Monad[F].unit
    } yield ()
  }

  def createSnapshot(lastIndex: Long)(using MonadThrow[F], Logger[F]): F[Snapshot[T]] = {
    for {
      _ <- trace"Creating snapshot up to index: $lastIndex"
      currentState <- stateMachine.getCurrentState
      config <- membershipManager.getClusterConfiguration
      snapshot = Snapshot(lastIndex, currentState, config)
      _ <- snapshotStorage.persistSnapshot(snapshot)
      _ <- trace"Snapshot created and persisted for index: $lastIndex"
    } yield snapshot
  }

  def shouldCreateSnapshot()(using Monad[F]): F[Boolean] = {
    for {
      lastLogIndex <- logStorage.lastIndex
      appliedIndex <- stateMachine.appliedIndex
      lastSnapshot <- snapshotStorage.retrieveSnapshot
      lastSnapshotIndex = lastSnapshot.map(_.lastIndex).getOrElse(0L)
      logsSinceSnapshot = appliedIndex - lastSnapshotIndex
    } yield logsSinceSnapshot > 1000
  }

  def installSnapshot(snapshot: Snapshot[T])(using MonadThrow[F], Logger[F]): F[Unit] = {
    transactional {
      for {
        _ <- trace"Installing snapshot with last index: ${snapshot.lastIndex}"
        _ <- snapshotStorage.persistSnapshot(snapshot)
        _ <- restoreSnapshot(snapshot)
        _ <- logStorage.deleteAfter(snapshot.lastIndex)
        _ <- setCommitIndex(snapshot.lastIndex)
        _ <- trace"Snapshot installed successfully"
      } yield ()
    }
  }

  def getSnapshotMetadata()(using Monad[F]): F[Option[(Long, ClusterConfiguration)]] = {
    for {
      snapshot <- snapshotStorage.retrieveSnapshot
    } yield snapshot.map(s => (s.lastIndex, s.config))
  } 

  def restoreSnapshot[T](snapshot: Snapshot[T])(using
      Monad[F],
      Logger[F]
  ): F[Unit] = 
    for {
      _ <- trace"Restoring snapshot with last index: ${snapshot.lastIndex}"
      _ <- membershipManager.setClusterConfiguration(snapshot.config)
      _ <- stateMachine.restoreSnapshot(snapshot.lastIndex, snapshot.data)
      _ <- setCommitIndex(snapshot.lastIndex)
      _ <- trace"Snapshot restored successfully"
    } yield ()