package com.grok.raft.core.internal

trait Log[F[_]] {

	def state(): F[LogState]

}