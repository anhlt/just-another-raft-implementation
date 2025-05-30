package com.grok.raft.core.internal

/**
 * A Raft‐specific deferred/promise that ties a client’s write command to the
 * eventual result of applying that command at a given log index.
 *
 * Internally, the `Log` implementation uses this to:
 *   1. Register the deferred when the entry is appended:
 *        – `Log.append(term, cmd, deferred)` stores `deferred` in a map under `index`.
 *   2. Observe replication and commitment:
 *        – Once a quorum has replicated the entry, `Log.commitLogs` calls
 *          `commitLog(index)`.
 *   3. Complete and clean up the deferred:
 *        – After applying the command to the state machine, `commitLog` invokes
 *          `deferred.complete(result)` and removes it from the map to avoid leaks.
 */
trait RaftDeferred[F[_], A] :

  def get: F[A]

  def complete(a: A): F[Boolean]