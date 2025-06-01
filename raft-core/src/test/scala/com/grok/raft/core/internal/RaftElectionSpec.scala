package com.grok.raft.core.internal
import cats.*
import cats.effect.*
import cats.implicits.*
import com.grok.raft.core.*
import org.typelevel.log4cats.Logger
import com.grok.raft.core.internal.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*
import com.grok.raft.core.storage.*

class TestRaft[F[_]: Async](
    override val config: ClusterConfiguration,
    override val leaderAnnouncer: LeaderAnnouncer[F],
    override val membershipManager: MembershipManager[F],
    override val logPropagator: LogPropagator[F],
    override val log: Log[F],
    override val stateStorage: StateStorage[F],
    override val rpcClient: RpcClient[F]
) extends Raft[F] {

  override def deferred[A]: F[RaftDeferred[F, A]] = {

    val emptyD = new RaftDeferred[F, A] {

      val deffered = Deferred.unsafe[F, A]

      def get: F[A] = deffered.get

      def complete(a: A) = deffered.complete(a)
    }
    Monad[F].pure(emptyD)
  }

  override def background[A](fa: => F[A])(using MonadThrow[F]): F[Unit] = fa >> Async[F].unit

  override def schedule(delay: FiniteDuration)(fa: => F[Unit])(using Monad[F]): F[Unit] = fa >> Async[F].unit

  val isRunning: Ref[F, Boolean]                                  = Ref.unsafe[F, Boolean](true)
  override def setRunning(r: Boolean): F[Unit]                    = isRunning.set(r)
  override def getRunning: F[Boolean]                             = isRunning.get

  val currentNodeRef: Ref[F, Node] = Ref.unsafe[F, Node](config.currentNode)

  override def currentNode: F[Node]                               = currentNodeRef.get
  override def setCurrentNode(n: Node): F[Unit]                   = currentNodeRef.set(n)
  override def delayElection()(using Monad[F]): F[Unit]           = Monad[F].unit
  override def electionTimeoutElapsed(using Monad[F]): F[Boolean] = Monad[F].pure(false)

  // No‐ops for scheduling/heartbeats
  def scheduleElection()(using Monad[F], Logger[F]): F[Unit]    = Monad[F].unit
  def scheduleReplication()(using Monad[F], Logger[F]): F[Unit] = Monad[F].unit
  def updateLastHeartbeat(using Monad[F], Logger[F]): F[Unit]   = Monad[F].unit
}

class RaftElectionSpec extends CatsEffectSuite {

  // a no‐op logger for simplicity

  // three in-memory node addresses
  private val n1 = NodeAddress("n1", 9090)
  private val n2 = NodeAddress("n2", 9090)
  private val n3 = NodeAddress("n3", 9090)

  // cluster config with n1 as the starting follower
  private val baseConfig = ClusterConfiguration(
    currentNode = Candidate(currentTerm = 0L, address = n1),
    members = List(n1, n2, n3)
  )

  test("TestRaft should elect a leader when majority votes yes") {
    // 1) Set up a StubLeaderAnnouncer that we can .listen()

    // def makeAnnouncer[F[_]: Async]: IO[LeaderAnnouncer[F]] = StubLeaderAnnouncer.create[IO]
    // 2) StubRpcClient: only n2 & n3 give us votes
    def makeRpcClient[F[_]: Async]: RpcClient[F] =
      new StubRpcClient[F](voteMap = Map(n2 -> true, n3 -> true))

    for {
      announcer <- StubLeaderAnnouncer.create[IO]
      rpcClient = makeRpcClient[IO]

      // 3) Build our TestRaft, overriding schedule to run immediately
      raft = new TestRaft[IO](
        config = baseConfig,
        leaderAnnouncer = announcer,
        membershipManager = new DummyMembershipManager[IO],
        logPropagator = new DummyLogPropagator[IO],
        log = new InMemoryLog[IO],
        stateStorage = new InMemoryStateStorage[IO],
        rpcClient = rpcClient
      )

      // 4) kick off rafting — this will (a) runElection(), (b) collect votes, (c) announce leader
      _ <- raft.start()

      // 5) block until our announcer completes with a leader
      leaderAddress <- announcer.listen()
      currentNode <- raft.currentNode
    } yield {
      assert(
        currentNode.isInstanceOf[Leader],
        s"Expected a Leader but got ${currentNode}"
      )
      // optionally inspect term / address
      val Leader(addr, term, _, _, _) = currentNode.asInstanceOf[Leader]
      assertEquals(addr, n1, "n1 should have self-elected as leader on term 1")
      assert(term > 0, "Term should have been bumped by the election")
    }
  }
}
