package com.grok.raft.core.internal


/**
  * Represents a single entry in the Raft log.
  */
trait LogEntry {
  val term: Int
  val index: Int
}
case class LogEntryGeneral[T](term: Int, index: Int, command: T) extends LogEntry