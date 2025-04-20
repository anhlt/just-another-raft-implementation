package com.grok.raft.core.internal

/** Represents a single entry in the Raft log.
  */
trait LogEntry {
  val term: Long
  val index: Long
}

case class LogEntryGeneral[T](term: Long, index: Long, command: T) extends LogEntry
