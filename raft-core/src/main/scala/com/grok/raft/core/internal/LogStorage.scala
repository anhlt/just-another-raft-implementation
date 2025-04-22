package com.grok.raft.core.internal

trait LogStorage[F[_]]:

  def currentLength: F[Long]

  def getAtLength(lenght: Long): F[LogEntry] = get(lenght - 1)

  def get(index: Long): F[LogEntry]

  def put(index: Long, logEntry: LogEntry): F[LogEntry]

  def deleteBefore(index: Long): F[Unit]

  def deleteAfter(index: Long): F[Unit]
