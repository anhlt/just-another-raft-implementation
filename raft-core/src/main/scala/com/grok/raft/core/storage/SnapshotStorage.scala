package com.grok.raft.core.storage

trait SnapshotStorage[F[_], T] {

  /** Persists the snapshot to the storage.
    *
    * @param snapshot
    *   The snapshot to persist.
    * @return
    *   A monadic effect that completes when the snapshot is persisted.
    */
  def persistSnapshot(snapshot: Snapshot[T]): F[Unit]

  /** Retrieves the latest snapshot from the storage.
    *
    * @return
    *   A monadic effect containing an optional snapshot.
    */
  def retrieveSnapshot: F[Option[Snapshot[T]]]

  /** Get latest snapshot
    *
    * @return
    *   A monadic effect containing the latest snapshot.
    */
  def getLatestSnapshot: F[Snapshot[T]]

}
