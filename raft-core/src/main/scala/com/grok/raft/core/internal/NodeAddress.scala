package com.grok.raft.core.internal

case class NodeAddress(ip: String, port: Int) {
  override def toString: String = s"$ip:$port"
  def id: String                = s"$ip:$port"
}
