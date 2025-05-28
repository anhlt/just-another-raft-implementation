package com.grok.raft.core.error

trait Error extends Throwable

case class RaftError(message: String) extends Error {
  override def toString: String = s"RaftError: $message"
}

case class LogError(message: String) extends Error {
  override def toString: String = s"LogError: $message"
}

case class MembershipError(message: String) extends Error {
  override def toString: String = s"MembershipError: $message"
}
