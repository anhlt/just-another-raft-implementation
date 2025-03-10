package com.grok.raft.core.internal

/** State of the log
  *
  * @param lastLogIndex
  *   the index of the last log entry
  * @param lastLogTerm
  *   the term of the last log entry
  * @param lastAppliedIndex
  *   the index of the last applied log entry
  */

case class LogState(lastLogIndex: Long, lastLogTerm: Option[Long], lastAppliedIndex: Long = 0)
