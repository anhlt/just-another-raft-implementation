package com.grok.raft.core.internal.storage

import com.grok.raft.core.internal._

trait LogStorage[F[_]]:

  def currentLength: F[Long]

  def getAtLength(lenght: Long): F[Option[LogEntry]] = get(lenght - 1)

  def get(index: Long): F[Option[LogEntry]]

  def put(index: Long, logEntry: LogEntry): F[LogEntry]

  def deleteBefore(index: Long): F[Unit]

  def deleteAfter(index: Long): F[Unit]

  def truncateList(maxLenght: Long): F[Unit] = deleteAfter(maxLenght - 1)
