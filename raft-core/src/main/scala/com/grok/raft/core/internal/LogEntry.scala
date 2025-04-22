package com.grok.raft.core.internal

import com.grok.raft.core.Command

/** Represents a single entry in the Raft log.
  */
trait LogEntry {
  val term: Long
  val index: Long
  val command: Command[?]
}

case class LogEntryGeneral(term: Long, index: Long, command: Command[?]) extends LogEntry
