package com.grok.raft.core.storage


trait StateStorage[F[_]] {

  def persistState(state: PersistedState): F[Unit]

  def retrieveState(): F[Option[PersistedState]]
}