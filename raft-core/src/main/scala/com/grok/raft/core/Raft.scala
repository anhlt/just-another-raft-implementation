package com.grok.raft.core

import cats._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*
import com.grok.raft.core.internal._
import cats.implicits._
import com.grok.raft.core.protocol.Action
import scala.concurrent.duration._





trait Raft[F[_]] {

  val config: ClusterConfiguration

  val leaderAnnouncer: LeaderAnnouncer[F]

  val membershipManager: MembershipManager[F]

  val log: Log[F]


  def setRunning(running: Boolean): F[Unit]

  def getRunning(): F[Boolean]

  def currentNode(): F[Node]

  def setCurrentNode(node: Node): F[Unit]

  def updateLastHeartbeat()(using Monad[F], Logger[F]): F[Unit]

  def electionTimeoutElapsed(using Monad[F]) : F[Boolean]

  // Function to randomly delay the election
  def delayElection()(using Monad[F]): F[Unit]


  def start()(using Monad[F], Logger[F]): F[Unit] = {
    for {
      _    <- info"Starting Raft"
      _    <- delayElection()
      node <- currentNode()
      _    <- if (node.leader().isDefined) Monad[F].unit else runElection()
      _    <- scheduleElection()
      _    <- scheduleReplication()
      _    <- info"Raft started"
      leader <- leaderAnnouncer.listen()
      _      <- info"Leader elected $leader"
    } yield ()
  }

  def runElection()(using Monad[F], Logger[F]): F[Unit] =
    for {
      _        <- delayElection()
      logState <- log.state()
      cluster  <- membershipManager.getClusterConfiguration
      actions  <- modifyState(_.onTimer(logState, cluster))
      _        <- runActions(actions)
    } yield ()

  def modifyState[B](f: Node => (Node, B))(using Monad[F], Logger[F]): F[B] =
    for {
      currentState <- currentNode()
      (newState, actions) = f(currentState)
      _ <- setCurrentNode(newState)
    } yield (actions)

  def runActions(actions: List[Action])(using Monad[F]): F[Unit] =
    actions.traverse(action => runAction(action)) *> Monad[F].unit

  def runAction(action: Action): F[Unit] = ???

  def scheduleElection()(using Monad[F], Logger[F]): F[Unit] = {
    background {
      schedule(config.heartbeatTimeoutMillis.milliseconds) {
        for {
          alive <- electionTimeoutElapsed
          _     <- if (alive) Monad[F].unit else runElection()
        } yield ()
      }
    }
  }

  def scheduleReplication()(using Monad[F], Logger[F]): F[Unit] = {
    background {
      schedule(config.heartbeatIntervalMillis.milliseconds) {
        for {
          node   <- currentNode()
          config <- membershipManager.getClusterConfiguration
          actions = if (node.isInstanceOf[Leader]) node.onReplicateLog(config) else List.empty
          _ <- runActions(actions)
        } yield ()
      }
    }
  }


  def background[A](fa: => F[A])(using Monad[F]): F[Unit]

  def schedule(delay: FiniteDuration)(fa: => F[Unit])(using Monad[F]): F[Unit]

}
