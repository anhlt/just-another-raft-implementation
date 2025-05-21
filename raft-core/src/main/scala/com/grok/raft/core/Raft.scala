package com.grok.raft.core

import cats._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax._
import com.grok.raft.core.internal._
import cats.implicits._
import com.grok.raft.core.protocol._
import scala.concurrent.duration._

trait Raft[F[_]] {

  val config: ClusterConfiguration

  val leaderAnnouncer: LeaderAnnouncer[F]

  val membershipManager: MembershipManager[F]

  val logPropagator: LogPropagator[F]

  val log: Log[F]

  def setRunning(running: Boolean): F[Unit]

  def getRunning: F[Boolean]

  def currentNode: F[Node]

  def setCurrentNode(node: Node): F[Unit]

  def updateLastHeartbeat(using Monad[F], Logger[F]): F[Unit] = ???

  def electionTimeoutElapsed(using Monad[F]): F[Boolean]

  // Function to randomly delay the election
  def delayElection()(using Monad[F]): F[Unit]

  def rpcClient: RpcClient[F]

  def start()(using Monad[F], Logger[F]): F[Unit] = {
    for {
      _      <- trace"Starting Raft"
      _      <- delayElection()
      node   <- currentNode
      _      <- if (node.leader().isDefined) Monad[F].unit else runElection()
      _      <- scheduleElection()
      _      <- scheduleReplication()
      _      <- trace"Raft started"
      leader <- leaderAnnouncer.listen()
      _      <- trace"Leader elected $leader"
    } yield ()
  }

  def runElection()(using Monad[F], Logger[F]): F[Unit] =
    for {
      _        <- delayElection()
      logState <- log.state
      cluster  <- membershipManager.getClusterConfiguration
      actions  <- modifyState(node => node.onTimer(logState, cluster))
      _        <- runActions(actions)
    } yield ()

  def modifyState[B](f: Node => (Node, B))(using Monad[F], Logger[F]): F[B] =
    for {
      currentState <- currentNode
      (newState, actions) = f(currentState)
      _ <- setCurrentNode(newState)
    } yield (actions)

  def runActions(actions: List[Action])(using Monad[F], Logger[F]): F[Unit] =
    actions.traverse(action => runAction(action)) *> Monad[F].unit

  def runAction(action: Action)(using Monad[F], Logger[F]): F[Unit] = {
    action match {
      case reqForVote: RequestForVote =>
        for {
          _        <- trace"Sending a vote request to ${reqForVote}"
          response <- rpcClient.send(reqForVote.peerId, reqForVote.request)
          _        <- onVoteResponse(response)
        } yield ()
      case replicateLog: ReplicateLog =>
        background {
          for {
            response <- logPropagator.propagateLogs(replicateLog.peerId, replicateLog.term, replicateLog.prefixLength)
            _        <- onLogRequestResponse(response)
          } yield ()
        }

      case CommitLogs(ackLengthMap) =>
        for {
          committed <- log.commitLogs(ackLengthMap)
          _         <- if (committed) storeState() else Monad[F].unit
        } yield ()
      case announceLeader: AnnounceLeader => ???
      case ResetLeaderAnnouncer           => ???
      case StoreState                     => ???

    }
  }

  def onLogRequest(msg: LogRequest)(using Monad[F], Logger[F]): F[LogRequestResponse] = {
    for {
      _        <- trace"A AppendEntriesRequest received from ${msg.leaderId} with term ${msg.term}"
      logState <- log.state
      config   <- membershipManager.getClusterConfiguration
      logPrevSent <- log.get(msg.prevSentLogLength - 1)
      (response, actions)<- modifyState(_.onLogRequest(msg, logState,logPrevSent,  config))
      _ <- updateLastHeartbeat
      _        <- runActions(actions)
              appended <-
          if (response.success) {
            for {
              appended <- log.appendEntries(msg.entries, msg.prevLogIndex, msg.leaderCommit)
            } yield appended
          } else
            Monad[F].pure(false)

    } yield response
  }



  def onLogRequestResponse(msg: LogRequestResponse)(using Monad[F], Logger[F]): F[Unit] =
    for {
      _        <- trace"A AppendEntriesResponse received from ${msg.nodeId}. ${msg}"
      logState <- log.state
      config   <- membershipManager.getClusterConfiguration
      actions  <- modifyState(_.onLogRequestResponse(logState, config, msg))
      _        <- trace"Actions ${actions}"
      _        <- runActions(actions)
    } yield ()

  def onVoteRequest(msg: VoteRequest)(using Monad[F], Logger[F]): F[VoteResponse] = {
    for {
      _                   <- trace"A Vote request received from ${msg.nodeAddress}, Term: ${msg.lastLogTerm}, ${msg}"
      logState            <- log.state
      config              <- membershipManager.getClusterConfiguration
      (response, actions) <- modifyState(_.onVoteRequest(msg, logState, config))

      _ <- runActions(actions)
      _ <- trace"Vote response to the request ${response}"
      _ <- if (response.voteGranted) updateLastHeartbeat else Monad[F].unit
    } yield response
  }

  def onVoteResponse(msg: VoteResponse)(using Monad[F], Logger[F]): F[Unit] =
    for {
      _        <- trace"A Vote response received from ${msg.nodeAddress}, Granted: ${msg.voteGranted}, ${msg}"
      logState <- log.state
      config   <- membershipManager.getClusterConfiguration
      actions  <- modifyState((node: Node) => node.onVoteResponse(msg, logState, config))
      _        <- runActions(actions)
    } yield ()

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
          node   <- currentNode
          config <- membershipManager.getClusterConfiguration
          actions = if (node.isInstanceOf[Leader]) node.onReplicateLog(config) else List.empty
          _ <- runActions(actions)
        } yield ()
      }
    }
  }

  def background[A](fa: => F[A])(using Monad[F]): F[Unit]

  def schedule(delay: FiniteDuration)(fa: => F[Unit])(using Monad[F]): F[Unit]

  // Function to create a deferred computation
  def deferred[A]: F[Deferred[F, A]] = ???

  def onCommand[T](c: Command[T])(using Monad[F], Logger[F]): F[T] = c match {
    case cmd: ReadCommand[T] =>
      for {
        node <- currentNode
        result <- node match
          case leader: Leader => log.applyReadCommand(cmd)
          case _ =>
            for {
              leaderNode <- leaderAnnouncer.listen()
              rs         <- rpcClient.send(leaderNode.address, cmd)
            } yield rs
      } yield (result)

    case cmd: WriteCommand[T] =>
      for {
        deferred <- deferred[T]
        node     <- currentNode
        config   <- membershipManager.getClusterConfiguration
        actions  <- onWriteCommand(node, cmd, deferred)
        _        <- runActions(actions)
        result   <- deferred.get
      } yield (result)
  }

  /** Processes a write command on the given node by executing the command logic and producing a list of follow-up
    * actions.
    *
    * This method takes a write command along with a Deferred value that will eventually hold the result of the
    * operation. It performs the command's execution in the context of the given node by applying necessary validations,
    * state updates, or other side effects. Once the command is processed, it returns an effectful list of actions that
    * are scheduled as a result of executing the command. The deffered value is used to signal the completion of the
    * command processing. When the command being appending to log and the node also propragates the ReplicateLog command
    * to the follower nodes. When the leader receives the response from the follower nodes, it will check if the quorum
    * is reached and then commit the command. After the command is committed, the leader will complete the deferred
    * value with the result of the command.
    *
    * @param node
    *   The node on which the write command is executed.
    * @param cmd
    *   The write command carrying the necessary information of type T for the operation.
    * @param deferred
    *   A Deferred reference used to provide the outcome of type T once the command is processed.
    * @tparam T
    *   The type parameter representing the result type of the write command.
    * @return
    *   A wrapped effect that yields a list of actions to be performed after the command is processed.
    */
  private def onWriteCommand[T](node: Node, cmd: WriteCommand[T], deferred: Deferred[F, T])(using
      Monad[F],
      Logger[F]
  ): F[List[Action]] = {
    node match
      case leader: Leader => {
        for {
          _ <- trace"Appending the command to the log ${config.members}"
          _ <- log.append(leader.currentTerm, cmd, deferred)

        } yield node.onReplicateLog(config)
      }
      case _ => {
        for {
          _        <- trace"Follower received write command"
          leader   <- leaderAnnouncer.listen()
          _        <- trace"The current leader is ${leader}."
          response <- rpcClient.send(leader.address, cmd)
          _        <- trace"Response for the write command received from the leader"
          actions  <- deferred.complete(response)
        } yield List.empty
      }
  }


  def storeState()(using Monad[F], Logger[F]): F[Unit] = ???
}
