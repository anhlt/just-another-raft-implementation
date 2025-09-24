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

class RaftImlp[F[_]: {Sync, Temporal}, T](
    val config: ClusterConfiguration,
    val leaderAnnouncer: LeaderAnnouncer[F],
    val membershipManager: MembershipManager[F],
    val log: Log[F, T],
    val rpcClient: RpcClient[F],
    val logPropagator: LogPropagator[F],
    val stateStorage: StateStorage[F],
    currentStateRef: Ref[F, Node],
    isRunning: Ref[F, Boolean],
    lastHeartbeatRef: Ref[F, Long],
) extends Raft[F, T]:

  override def deferred[A]: F[RaftDeferred[F, A]] = 
    for {
      catsDeferred <- Deferred[F, A]
    } yield new DeferredImpl[F, A](catsDeferred)

  // Remove the override since storeState is already implemented in the trait
  // override def storeState(using Monad[F], Logger[F]): F[Unit] = ???

  override def background[A](fa: => F[A])(using MonadThrow[F]): F[Unit] = 
    Temporal[F].start(fa) *> Monad[F].unit

  override def electionTimeoutElapsed(using Monad[F]): F[Boolean] = 
    for {
      lastHeartbeat <- lastHeartbeatRef.get
      currentTime <- Temporal[F].monotonic
      elapsed = currentTime.toMillis - lastHeartbeat
      node <- currentNode
    } yield elapsed < config.heartbeatTimeoutMillis || node.isInstanceOf[Leader]
  

  override def setRunning(running: Boolean): F[Unit] = isRunning.set(running)


  override def getRunning: F[Boolean] = isRunning.get

  override def updateLastHeartbeat(using Monad[F], Logger[F]): F[Unit] = {
    for  {
      _ <- trace"Updating last heartbeat"
      currentTime <- Temporal[F].monotonic
      _ <- lastHeartbeatRef.set(currentTime.toMillis)
    } yield ()

  }

  override def setCurrentNode(node: Node): F[Unit] = currentStateRef.set(node)

  override def currentNode: F[Node] = currentStateRef.get

  override def delayElection()(using Monad[F]): F[Unit] = 
    for {
      millis <- Sync[F].delay(config.electionMinDelayMillis + scala.util.Random.nextInt(config.electionMaxDelayMillis - config.electionMinDelayMillis))
      delayTimes <- Sync[F].delay(millis.milliseconds)
      _ <- Temporal[F].sleep(delayTimes)
    } yield ()

  override def schedule(delay: FiniteDuration)(fa: => F[Unit])(using Monad[F]): F[Unit] =
    Monad[F]
      .foreverM({
        for {
          _ <- Temporal[F].sleep(delay)
          _ <- fa
        } yield ()
      })
      .whileM_(isRunning.get)
