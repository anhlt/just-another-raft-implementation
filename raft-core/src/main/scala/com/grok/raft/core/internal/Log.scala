package com.grok.raft.core.internal

import com.grok.raft.core._
import com.grok.raft.core.internal.storage._
import cats._
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*
import scala.collection.concurrent.TrieMap

trait Log[F[_]]:

  val logStorage: LogStorage[F]

  private val deferreds = TrieMap[Long, Deferred[F, Any]]()

  val membershipManager: MembershipManager[F]

  def transactional[A](t: => F[A]): F[A]

  val stateMachine: StateMachine[F]

  /**
   * Methods to access and modify the commit index.
   * The commit index represents the highest log entry known to be committed in the Raft consensus.
   * 
   * Implementation note: These operations should be atomic to ensure consistency
   * in a concurrent environment.
   */

  /**
   * Retrieves the current commit index.
   *
   * @return The current commit index wrapped in effect type F
   */
  def getCommittedLength: F[Long]


  /**
   * Updates the commit index to a new value.
   *
   * @param index The new commit index value
   * @return Unit wrapped in effect type F
   */
  def setCommitLength(index: Long): F[Unit]

  def state: F[LogState]

  def append[T](term: Long, command: Command[T], deferred: Deferred[F, T])(using Monad[F], Logger[F]): F[LogEntry] =
    transactional {
      for {
        lastIndex <- logStorage.currentLength
        logEntry = LogEntryGeneral(term, lastIndex + 1, command)
        _ <- trace"Appending a command to the log. Term: ${term}, Index: ${lastIndex + 1}"
        _ <- logStorage.put(logEntry.index, logEntry)
        _ = deferreds.put(logEntry.index, deferred.asInstanceOf[Deferred[F, Any]])
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
    * @param ackLengthMap
    *   A map of node addresses to their acknowledged log lengths
    * @return
    *   A wrapped boolean value indicating whether any new entries were committed (true) or not (false)
    */
  def commitLogs(ackLengthMap: Map[NodeAddress, Long])(using Monad[F], Logger[F]): F[Boolean] = {
    for {
      currentLength  <- logStorage.currentLength
      commitedLength <- getCommittedLength
      _              <- trace"Current length: $currentLength, Committed length: $commitedLength"
      _              <- trace"Received ackLengthMap: $ackLengthMap"
      _              <- trace"Checking for quorum"

      _ <- trace"Committing"
    } yield (true)
  }

  def commitIfMatch(ackedLengthMap: Map[NodeAddress, Long], lenght: Long)(using
      Monad[F],
      Logger[F]
  ): F[Unit] = {
    for {
      config <- membershipManager.getClusterConfiguration
      ackedCount = ackedLengthMap.count { case (_, length) => length >= lenght }
      _ <- trace"ackedCount: ${ackedCount}, config: ${config.members.size}"
      _ <- if (ackedCount >= config.quorumSize) commitLog(lenght) *> Monad[F].pure(true) else Monad[F].pure(false)
    } yield ()
  }

  def commitLog(lenght: Long)(using
      Monad[F],
      Logger[F]
  ): F[Unit] = {
    for {
      logEntry <- logStorage.getAtLength(lenght)
      _        <- trace"Committing log entry: ${logEntry}"
      _        <- logStorage.deleteBefore(logEntry.index)
      _        <- applyCommand(logEntry.index, logEntry.command)
      _        <- setCommitLength(logEntry.index + 1)
      _        <- trace"Log entry committed: ${logEntry}"
    } yield ()
  }

  def applyCommand(index: Long, command: Command[?])(using Monad[F]): F[Unit] = {
    val output = command match {

      case command: ReadCommand[_] =>
        stateMachine.applyRead.apply(command)

      case command: WriteCommand[_] =>
        stateMachine.applyWrite.apply((index, command))
    }

    output.flatMap(result =>
      deferreds.get(index) match {
        case Some(deferred) => deferred.complete(result) *> Monad[F].pure(deferreds.remove(index))
        case None           => Monad[F].unit
      }
    )
  }
