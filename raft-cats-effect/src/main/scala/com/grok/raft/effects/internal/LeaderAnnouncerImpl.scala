package com.grok.raft.effects.internal

import com.grok.raft.core.internal.LeaderAnnouncer
import cats.effect.{Deferred, Ref}
import com.grok.raft.core.internal.Node
import cats.implicits._
import cats.effect.kernel._
import cats._
import cats.effect._

/**
 * Implementation of the LeaderAnnouncer trait using Cats Effect concurrency primitives.
 *
 * This class manages leadership announcements in a Raft cluster environment.
 * It uses a Ref to hold a Deferred promise representing the current leadership announcement.
 * 
 * - `announce` completes the Deferred with the new leader, unblocking all listeners.
 * - `listen` waits on the current Deferred to get the latest leader.
 * - `reset` replaces the current Deferred with a fresh one to prepare for a new leader announcement.
 *
 * This design ensures:
 *  - Safe concurrent access to leader state.
 *  - Listeners block efficiently until a leader is announced.
 *  - Leadership changes are coordinated and visible to all listeners.
 *
 * @tparam F effect type with concurrent capabilities
 */
class LeaderAnnouncerImpl[F[_]: Concurrent](ref: Ref[F, Deferred[F, Node]]) extends LeaderAnnouncer[F]:

  /**
   * Listen for the next leader announcement.
   * This method obtains the current Deferred and waits for its completion.
   * If the leader is already announced, the returned effect completes immediately.
   *
   * @return an effect producing the current or next leader
   */
  override def listen(): F[Node] = 
    for {
      currentDeferred <- ref.get                  // Retrieve current Deferred
      leader <- currentDeferred.get               // Suspend until Deferred is completed
    } yield leader

  /**
   * Announce a newly elected leader.
   * Completes the current Deferred promise, unblocking all listeners waiting on `listen`.
   * If the Deferred is already completed, this call is a no-op.
   *
   * @param leader the newly elected leader node
   * @return effect signaling completion of announcement
   */
  override def announce(leader: Node): F[Unit] = 
    for {
      currentDeferred <- ref.get                   // Retrieve current Deferred
      _ <- currentDeferred.complete(leader)       // Complete Deferred to announce leader
    } yield ()

  /**
   * Reset the announcer for a new leader.
   * Creates a fresh Deferred promise and installs it into the Ref,
   * so listeners that call `listen` afterward will await the next announcement.
   * 
   * This is typically called when observing a new term or leadership change,
   * to invalidate previously announced leaders.
   * 
   * @return effect signaling completion of reset
   */
  override def reset(): F[Unit] = 
    for {
      newDeferred <- Deferred[F, Node]             // Create new empty Deferred
      _ <- ref.set(newDeferred)                     // Replace current Deferred atomically
    } yield ()