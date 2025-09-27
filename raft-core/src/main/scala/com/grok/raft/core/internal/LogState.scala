package com.grok.raft.core.internal

/** Represents the state of the log.
  *
  * @param lastLogIndex
  *   the index of the last log entry (0-based), or -1 if no entries exist
  * @param lastLogTerm
  *   the term of the most recent log entry.
  * @param appliedLogIndex
  *   the index of the last applied log entry.
  */

case class LogState(lastLogIndex: Long, lastLogTerm: Option[Long], appliedLogIndex: Long = -1) {

  /** Returns the length of the log (count of entries) */
  def logLength: Long = lastLogIndex + 1
}
