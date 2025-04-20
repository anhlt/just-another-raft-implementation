package com.grok.raft.core.internal

import com.grok.raft.core._
import cats._
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*
import scala.collection.concurrent.TrieMap



trait Log[F[_]]:

  val logStorage: LogStorage[F]

  private val deferreds = TrieMap[Long, Deferred[F, Any]]()


  def transactional[A](t: => F[A]): F[A]

  def state: F[LogState]

  def append[T](term: Long, command: Command[T], deferred: Deferred[F, T])(using Monad[F], Logger[F]): F[LogEntry] =
    transactional {
      for {
        lastIndex <- logStorage.lastIndex
        logEntry = LogEntryGeneral(term, lastIndex + 1, command)
        _ <- trace"Appending a command to the log. Term: ${term}, Index: ${lastIndex + 1}"
        _ <- logStorage.put(logEntry.index, logEntry)
        _ = deferreds.put(logEntry.index, deferred.asInstanceOf[Deferred[F, Any]])
        _ <- trace"Entry appended. Term: ${term}, Index: ${lastIndex + 1}"
      } yield logEntry
    }
