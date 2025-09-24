package com.grok.raft.effects

import com.grok.raft.core.Raft
import com.grok.raft.effects.internal.DeferredImpl
import cats._
import cats.effect._
import cats.implicits._
import scala.concurrent.duration._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*
import com.grok.raft.core.*
import com.grok.raft.core.storage.*
import com.grok.raft.core.internal.*

class RaftImlp[F[_]: Async, T, W](
    val config: ClusterConfiguration,
    val leaderAnnouncer: LeaderAnnouncer[F],
    val membershipManager: MembershipManager[F],
    val log: Log[F, T, W],
    val rpcClient: RpcClient[F],
    val logPropagator: LogPropagator[F],
    val stateStorage: StateStorage[F],
    currentStateRef: Ref[F, Node],
    isRunning: Ref[F, Boolean],
    lastHeartbeatRef: Ref[F, Long],
) extends Raft[F, T, W]:

  override def deferred[A]: F[RaftDeferred[F, A]] = 
    Deferred[F, A].map(new DeferredImpl[F, A](_))

  // Remove the override since storeState is already implemented in the trait
  // override def storeState(using Monad[F], Logger[F]): F[Unit] = ???

  override def background[A](fa: => F[A])(using MonadThrow[F]): F[Unit] = 
    Async[F].start(fa) *> Monad[F].unit

  override def electionTimeoutElapsed(using Monad[F]): F[Boolean] = 
    for {
      lastHeartbeat <- lastHeartbeatRef.get
      currentTime <- Async[F].monotonic
      elapsed = currentTime.toMillis - lastHeartbeat
      node <- currentNode
    } yield elapsed < config.heartbeatTimeoutMillis || node.isInstanceOf[Leader]
  

  override def setRunning(running: Boolean): F[Unit] = isRunning.set(running)


  override def getRunning: F[Boolean] = isRunning.get

  override def updateLastHeartbeat(using Monad[F], Logger[F]): F[Unit] = {
    for  {
      _ <- trace"Updating last heartbeat"
      currentTime <- Async[F].monotonic
      _ <- lastHeartbeatRef.set(currentTime.toMillis)
    } yield ()

  }

  override def setCurrentNode(node: Node): F[Unit] = currentStateRef.set(node)

  override def currentNode: F[Node] = currentStateRef.get

  override def delayElection()(using Monad[F]): F[Unit] = 
    for {
      millis <- Async[F].delay(config.electionMinDelayMillis + scala.util.Random.nextInt(config.electionMaxDelayMillis - config.electionMinDelayMillis))
      delayTimes <- Async[F].delay(millis.milliseconds)
      _ <- Async[F].sleep(delayTimes)
    } yield ()

  override def schedule(delay: FiniteDuration)(fa: => F[Unit])(using Monad[F]): F[Unit] =
    Monad[F]
      .foreverM({
        for {
          _ <- Async[F].sleep(delay)
          _ <- fa
        } yield ()
      })
      .whileM_(isRunning.get)
