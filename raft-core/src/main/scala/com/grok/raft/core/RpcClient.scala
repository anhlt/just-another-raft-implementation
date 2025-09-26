package com.grok.raft.core

import com.grok.raft.core.protocol.*
import com.grok.raft.core.internal.*


trait RpcClient[F[_]] {

  def send(serverId: NodeAddress, voteRequest: VoteRequest): F[VoteResponse]

  def send(serverId: NodeAddress, appendEntries: LogRequest): F[LogRequestResponse]

  // def send(serverId: Node, snapshot: Snapshot, lastEntry: LogEntry): F[AppendEntriesResponse]

  def send[T](serverId: NodeAddress, command: Command): F[T]

  def join(serverId: NodeAddress, newNode: NodeAddress): F[Boolean]

  def closeConnections(): F[Unit]
}