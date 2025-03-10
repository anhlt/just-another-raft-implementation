package com.grok.raft.core.storage

import com.grok.raft.core.internal.NodeAddress

case class PersistedState(term: Long, votedFor: Option[NodeAddress], appliedIndex: Long) {}
