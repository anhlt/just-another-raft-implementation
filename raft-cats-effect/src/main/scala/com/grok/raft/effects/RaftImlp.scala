package com.grok.raft.effects

import com.grok.raft.core.Raft
import cats._
import cats.effect._
import cats.mtl.Raise
import cats.implicits._
import scala.concurrent.duration._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*
import com.grok.raft.core.*
import com.grok.raft.core.storage.*
import com.grok.raft.core.internal.*
import com.grok.raft.core.error.RaftError
import com.grok.raft.effects.internal.DeferredImpl

class RaftImlp[F[_]: {Async, Temporal}, T](
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

  override def setRunning(running: Boolean): F[Unit] = 
    isRunning.set(running)

  override def getRunning: F[Boolean] = 
    isRunning.get

  override def currentNode: F[Node] = 
    currentStateRef.get

  override def setCurrentNode(node: Node): F[Unit] = 
    currentStateRef.set(node)

  override def updateLastHeartbeat(using Monad[F], Logger[F]): F[Unit] = 
    for
      currentTime <- Async[F].delay(System.currentTimeMillis())
      _ <- lastHeartbeatRef.set(currentTime)
      _ <- trace"Updated last heartbeat to $currentTime"
    yield ()

  override def electionTimeoutElapsed(using Monad[F]): F[Boolean] = 
    for
      currentTime <- Async[F].delay(System.currentTimeMillis())
      lastHeartbeat <- lastHeartbeatRef.get
      elapsed = currentTime - lastHeartbeat
      timeoutElapsed = elapsed > config.heartbeatTimeoutMillis
    yield !timeoutElapsed

  override def delayElection()(using Monad[F]): F[Unit] = 
    for
      delay <- Async[F].delay(scala.util.Random.between(config.electionMinDelayMillis, config.electionMaxDelayMillis))
      _ <- Temporal[F].sleep(delay.milliseconds)
    yield ()

  override def deferred[A]: F[RaftDeferred[F, A]] = 
    cats.effect.Deferred[F, A].map(d => new DeferredImpl[F, A](d))

  override def storeState(using Monad[F], Logger[F]): F[Unit] =
    for
      _ <- trace"Storing the new state in the storage"
      logState <- log.state
      node <- currentNode
      _ <- stateStorage.persistState(node.toPersistedState.copy(appliedIndex = logState.appliedLogIndex))
    yield ()

  override def background[A](fa: => F[A])(using Monad[F], Raise[F, RaftError]): F[Unit] = Temporal[F].start(fa) *> Monad[F].unit

  override def schedule(delay: FiniteDuration)(fa: => F[Unit])(using Monad[F]): F[Unit] =
    Monad[F]
      .foreverM({
        for {
          _ <- Temporal[F].sleep(delay)
          _ <- fa
        } yield ()
      })
      .whileM_(isRunning.get)
