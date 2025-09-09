package com.grok.raft.core.internal

/**
 * Represents the state of the log.
 *
 *
 * @param logLength the length of the log (count of entries)
 * @param lastLogTerm  the term of the most recent log entry.
 * @param appliedLogIndex the index of the last applied log entry.
 */

case class LogState(logLength: Long, lastLogTerm: Option[Long], appliedLogIndex: Long = -1) {
  /** Returns the index of the last log entry (0-based), or -1 if no entries exist */
  def lastLogIndex: Long = logLength - 1
}