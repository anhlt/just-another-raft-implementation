package com.grok.raft.core.internal

/**
 * Represents the state of the log.
 *
 *
 * @param logLength the length of the log
 * @param lastLogTerm  the term of the most recent log entry.
 * @param appliedLogLength the index of the last applied log entry.
 */

case class LogState(logLength: Long, lastLogTerm: Option[Long], appliedLogLength: Long = 0)