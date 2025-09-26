package com.grok.raft.core.error

sealed abstract class BaseError(message: String)
  extends Exception(message)

case class RaftError(val message: String) extends BaseError(message) {
  override def toString: String = s"RaftError: $message"
}

case class LogError(val message: String) extends BaseError(message) {
  override def toString: String = s"LogError: $message"
}

case class MembershipError(val message: String) extends BaseError(message) {
  override def toString: String = s"MembershipError: $message"
}

case class StateMachineError(val message: String) extends BaseError(message) {
  override def toString: String = s"StateMachineError: $message"
}
