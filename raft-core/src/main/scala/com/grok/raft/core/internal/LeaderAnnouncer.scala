package com.grok.raft.core.internal


trait LeaderAnnouncer[F[_]] {

  def announce(leader: Node): F[Unit]

  def reset(): F[Unit]

  def listen(): F[Node]
}