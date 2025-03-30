package com.grok.raft.core.internal

trait MembershipManager[F[_]] {
  def members: F[Set[Node]]

  def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit]

  def getClusterConfiguration: F[ClusterConfiguration]
}
