package com.grok.raft.core.storage

import com.grok.raft.core.ClusterConfiguration

case class Snapshot[T](lastIndex: Long, data: T, config: ClusterConfiguration)