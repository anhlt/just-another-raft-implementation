package com.grok.raft.core.internal

import com.grok.raft.core.protocol._


trait LogPropagator[F[_]]:
	def propagateLogs(peerId: NodeAddress, term: Long, nextIndex: Long): F[LogRequestResponse]	
