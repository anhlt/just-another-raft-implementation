package com.grok.raft.core.internal

/** Typeclass for leader announcement and coordination in a Raft or distributed cluster.
  *
  * This provides a high-level interface for components that need to:
  *   - Announce the identity of the leader to interested listeners.
  *   - Allow processes to await leadership changes.
  *   - Reset internal state in response to term/cohort changes (e.g., on observing a higher election term).
  *
  * ## Background In Raft, the knowledge of the current leader is critical for both safety and responsiveness. When a
  * node learns of a higher term, or a possible leadership change, it must "forget" the old leader, reset related
  * election information, and prepare to discover or elect a new leader. This trait can be used (for example, alongside
  * an implementation using a Cats Effect [[cats.effect.std.Deferred]] and [[cats.effect.Ref]]) to provide safe
  * communication and synchronization between components or fibers regarding leadership.
  *
  * ## Usage Example A typical implementation will:
  *   - `announce`: Complete a promise (e.g., [[cats.effect.Deferred]]) notifying waiting listeners of the new leader.
  *   - `reset`: Swap in a new empty promise, so future listeners await the *next* leadership announcement.
  *   - `listen`: Block until a leader has been announced; used by processes requiring up-to-date knowledge of the
  *     leader.
  *
  * Both thread-safety and single-assignment are preserved by using `Ref` and `Deferred` as the building blocks in a
  * Cats Effect environment.
  */
trait LeaderAnnouncer[F[_]]:

  /** Announce the newly-elected leader to all listeners.
    *
    * After this call, all current and future calls to `listen` will complete with the given leader, until the announcer
    * is reset via `reset`. Typically called when a new leader is elected.
    *
    * @param leader
    *   The newly elected leader node.
    * @return
    *   An effect that completes once all listeners have been notified.
    */
  def announce(leader: Node): F[Unit]

  /** Reset the announcer, preparing to listen for a new leader.
    *
    * Typical usage is when the node observes a higher term or new election (see Raft §5.2), indicating the previously
    * known leader is no longer valid. This ensures that subsequent listeners will wait for the next leadership
    * announcement, and not receive obsolete information.
    *
    * @return
    *   An effect that completes once the internal state is reset.
    */
  def reset(): F[Unit]

  /** Listen for the next leader announcement.
    *
    * Returns the current leader if already known; otherwise, this will semantically block until a leader is announced.
    * It is safe to call `listen` multiple times; each call will return the next leader after the most recent reset.
    *
    * @return
    *   An effect that completes with the latest known leader.
    */
  def listen(): F[Node]


