package com.grok.raft.core.error

trait BaseError extends Throwable

case class RaftError(message: String) extends BaseError {
  override def toString: String = s"RaftError: $message"
}

case class LogError(message: String) extends BaseError {
  override def toString: String = s"LogError: $message"
}

case class MembershipError(message: String) extends BaseError {
  override def toString: String = s"MembershipError: $message"
}
