package com.grok.raft.core.internal

import cats.effect.*
import cats.effect.kernel.*
import cats.implicits.*
import cats.*

// Bring your domain types into scope
import com.grok.raft.core.internal.storage.LogStorage
import com.grok.raft.core.internal.*
import com.grok.raft.core.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.grok.raft.core.protocol.*
import com.grok.raft.core.storage.SnapshotStorage
import com.grok.raft.core.storage.Snapshot
import com.grok.raft.core.storage.StateStorage
import com.grok.raft.core.storage.PersistedState
import scala.collection.immutable.TreeMap
import java.util.Arrays

object NoOp extends ReadCommand[Unit]


object TestData {

  val addr1 = NodeAddress("n1", 9090)
  val addr2 = NodeAddress("n2", 9090)
  val addr3 = NodeAddress("n3", 9090)

  val leader = Leader(currentTerm = 1L, address = addr1)
  val follower1 = Follower(currentTerm = 1L, address = addr2)
  val follower2 = Follower(currentTerm = 1L, address = addr3)

}

// 1) An in‐memory LogStorage
class InMemoryLogStorage[F[_]: Sync] extends LogStorage[F] {

  override def deleteBefore(index: Long): F[Unit] =
    ref.update(_.filter { case (k, _) => k >= index })

  private val ref = Ref.unsafe[F, Map[Long, LogEntry]](Map.empty)

  override def get(index: Long): F[Option[LogEntry]] =
    ref.get.map(_.get(index))

  override def put(index: Long, entry: LogEntry): F[LogEntry] =
    ref.update(_ + (index -> entry)).as(entry)

  override def deleteAfter(index: Long): F[Unit] =
    ref.update(_.filter { case (k, _) => k <= index })

  override def lastIndex: F[Long] =
    ref.get.map(entries => if (entries.isEmpty) -1L else entries.keys.max)
}

class InMemoryStateMachine[F[_]: Sync, T] extends StateMachine[F, T] {
  private val stateRef = Ref.unsafe[F, T](null.asInstanceOf[T])
  private val indexRef = Ref.unsafe[F, Long](0L)

  override def applyWrite: PartialFunction[(Long, WriteCommand[?]), F[Any]] = { 
    case (index, _) => indexRef.set(index) *> Sync[F].pure(true) 
  }

  override def applyRead: PartialFunction[ReadCommand[?], F[Any]] = { _ => Sync[F].pure(true) }

  override def appliedIndex: F[Long] = indexRef.get

  override def restoreSnapshot[U](lastIndex: Long, data: U): F[Unit] = 
    stateRef.set(data.asInstanceOf[T]) *> indexRef.set(lastIndex)

  override def getCurrentState: F[T] = stateRef.get
}

class InMemoryKVStateMachine[F[_]: Sync] extends KVStateMachine[F] {
  // Use ByteString wrapper for proper ordering and equality
  private case class ByteString(bytes: Array[Byte]) {
    override def equals(obj: Any): Boolean = obj match {
      case other: ByteString => Arrays.equals(bytes, other.bytes)
      case _ => false
    }
    override def hashCode(): Int = Arrays.hashCode(bytes)
  }
  
  private implicit val byteStringOrdering: Ordering[ByteString] = 
    (x: ByteString, y: ByteString) => Arrays.compare(x.bytes, y.bytes)

  private val storeRef = Ref.unsafe[F, TreeMap[ByteString, Array[Byte]]](TreeMap.empty)
  private val indexRef = Ref.unsafe[F, Long](0L)

  override def put(key: Array[Byte], value: Array[Byte]): F[Unit] =
    storeRef.update(_ + (ByteString(key) -> value))

  override def get(key: Array[Byte]): F[Option[Array[Byte]]] =
    storeRef.get.map(_.get(ByteString(key)))

  override def delete(key: Array[Byte]): F[Unit] =
    storeRef.update(_ - ByteString(key))

  override def contains(key: Array[Byte]): F[Boolean] =
    storeRef.get.map(_.contains(ByteString(key)))

  override def range(startKey: Array[Byte], endKey: Array[Byte], limit: Option[Int]): F[List[(Array[Byte], Array[Byte])]] =
    storeRef.get.map { store =>
      val start = ByteString(startKey)
      val end = ByteString(endKey)
      val filtered = store.range(start, end).toList.map { case (k, v) => (k.bytes, v) }
      limit.fold(filtered)(filtered.take)
    }

  override def scan(prefix: Array[Byte], limit: Option[Int]): F[List[(Array[Byte], Array[Byte])]] =
    storeRef.get.map { store =>
      val prefixStr = ByteString(prefix)
      val filtered = store.rangeFrom(prefixStr).takeWhile { case (k, _) => 
        k.bytes.take(prefix.length).sameElements(prefix)
      }.toList.map { case (k, v) => (k.bytes, v) }
      limit.fold(filtered)(filtered.take)
    }

  override def keys(prefix: Option[Array[Byte]], limit: Option[Int]): F[List[Array[Byte]]] =
    storeRef.get.map { store =>
      val filtered = prefix match {
        case Some(p) => 
          val prefixStr = ByteString(p)
          store.rangeFrom(prefixStr).takeWhile { case (k, _) => 
            k.bytes.take(p.length).sameElements(p)
          }.keys.toList.map(_.bytes)
        case None => store.keys.toList.map(_.bytes)
      }
      limit.fold(filtered)(filtered.take)
    }

  override def appliedIndex: F[Long] = indexRef.get

  override def restoreSnapshot[T](lastIndex: Long, data: T): F[Unit] = {
    try {
      val kvData = data.asInstanceOf[Map[Array[Byte], Array[Byte]]]
      val treeMap = TreeMap.from(kvData.map { case (k, v) => ByteString(k) -> v })
      storeRef.set(treeMap) *> indexRef.set(lastIndex)
    } catch {
      case _: ClassCastException => 
        corruptedState("snapshot data is not a valid Map[Array[Byte], Array[Byte]]")
      case e: Exception => 
        operationFailed("restoreSnapshot", e.getMessage)
    }
  }

  override def getCurrentState: F[Map[Array[Byte], Array[Byte]]] = 
    storeRef.get.map(_.unsorted.map { case (k, v) => k.bytes -> v }.toMap)
}

class DummyMembershipManager[F[_]: Sync] extends MembershipManager[F]:

  val configurationRef: Ref[F, ClusterConfiguration] =
    Ref.unsafe[F, ClusterConfiguration](
      ClusterConfiguration(
        currentNode = TestData.leader ,
        members = List(TestData.addr1, TestData.addr2, TestData.addr3)
      )
    )

  override def members: F[Set[Node]] = Monad[F].pure(
    Set(
      TestData.leader,
      TestData.follower1,
      TestData.follower2
    )
  )

  override def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit] = configurationRef.set(newConfig)
  override def getClusterConfiguration: F[ClusterConfiguration] = configurationRef.get

given logger: Logger[IO] = Slf4jLogger.getLogger[IO]




// ----------------------------------------------------------------
// STUB LEADER ANNOUNCER
// ----------------------------------------------------------------
class StubLeaderAnnouncer[F[_]: Concurrent] private (deferred: Deferred[F, NodeAddress])
    extends LeaderAnnouncer[F] {

  override def announce(leader: NodeAddress): F[Unit] =
    // Complete only once; further completes are no‐ops
    deferred.complete(leader).void

  override def listen(): F[NodeAddress] =
    // Block until `announce` is called
    deferred.get

  override def reset(): F[Unit] =
    // Not used in our tests
    Concurrent[F].unit
}

object StubLeaderAnnouncer {
  def create[F[_]: Concurrent]: F[StubLeaderAnnouncer[F]] =
    Deferred[F, NodeAddress].map(new StubLeaderAnnouncer[F](_))
}



class StubRpcClient[F[_]: Sync](voteMap: Map[NodeAddress, Boolean]) extends RpcClient[F] {

  override def send[T](serverId: NodeAddress, command: Command): F[T] = ???

  override def join(serverId: NodeAddress, newNode: NodeAddress): F[Boolean] = ???

  override def closeConnections(): F[Unit] = ???

  override def send(peer: NodeAddress, req: VoteRequest): F[VoteResponse] =
    Sync[F].delay {
      VoteResponse(peer, req.candidateTerm, voteMap.getOrElse(peer, false))
    }

  override def send(peer: NodeAddress, req: LogRequest): F[LogRequestResponse] =
    Sync[F].pure(LogRequestResponse(peer, req.term, req.entries.length, true))
}

class DummyLogPropagator[F[_]: Sync] extends LogPropagator[F] {
  override def propagateLogs(
      peer: NodeAddress,
      term: Long,
      prefixLength: Long
  ): F[LogRequestResponse] =
    Sync[F].pure(LogRequestResponse(peer, term,  prefixLength, success = true))
}

class InMemoryLog[F[_]: Sync, T] extends Log[F, T] {
    override val logStorage = new InMemoryLogStorage[F]
    override val snapshotStorage = new InMemorySnapshotStorage[F, T]
    override val membershipManager = new DummyMembershipManager[F]
    override val stateMachine = new InMemoryStateMachine[F, T]

    // identity transaction
    override def transactional[A](t: => F[A]): F[A] = t

    // commit‐index stored in a Ref so we can observe it if we wanted
    private val commitRef = Ref.unsafe[F, Long](-1L)
    override def getCommittedIndex: F[Long] = commitRef.get
    override def setCommitIndex(i: Long): F[Unit] = commitRef.set(i)

    // Not used in these tests
    override def state: F[LogState] = Sync[F].pure(LogState(0, None, 0))

    // Allow access to internal methods for testing
    def getCommittedLength: F[Long] = Sync[F].pure(commitRef.get.asInstanceOf[F[Long]]).flatten
    def setCommitLength(index: Long): F[Unit] = commitRef.set(index)
}

class InMemoryStateStorage [F[_]: Sync] extends StateStorage[F] {

  private val ref = Ref.unsafe[F, PersistedState](PersistedState(0L, None, 0L))

  def persistState(state: PersistedState): F[Unit] = ref.set(state)

  def retrieveState: F[Option[PersistedState]] = 

    ref.get.map(Some(_))
}

class InMemorySnapshotStorage[F[_]: Sync, T] extends SnapshotStorage[F, T] {
  private val ref = Ref.unsafe[F, Option[Snapshot[T]]](None)

  override def persistSnapshot(snapshot: Snapshot[T]): F[Unit] = 
    ref.set(Some(snapshot))

  override def retrieveSnapshot: F[Option[Snapshot[T]]] = 
    ref.get

  override def getLatestSnapshot: F[Snapshot[T]] = 
    ref.get.flatMap {
      case Some(snapshot) => Sync[F].pure(snapshot)
      case None => Sync[F].raiseError(new RuntimeException("No snapshot available"))
    }
}