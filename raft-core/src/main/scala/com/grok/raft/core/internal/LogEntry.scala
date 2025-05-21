package com.grok.raft.core.internal

import com.grok.raft.core.Command

/** Represents a single entry in the Raft log.
  */
trait LogEntryI {
  val term: Long
  val index: Long
  val command: Command[?]
  val position: Long = index + 1
} 

case class LogEntry(term: Long, index: Long, command: Command[?]) extends LogEntryI
