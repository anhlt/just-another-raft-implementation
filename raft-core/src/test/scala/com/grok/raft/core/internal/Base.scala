package com.grok.raft.core.internal

import cats.effect.*
import cats.effect.kernel.Ref
import cats.implicits.*
import cats.*

// Bring your domain types into scope
import com.grok.raft.core.internal.storage.LogStorage
import com.grok.raft.core.internal.*
import com.grok.raft.core.*
import com.grok.raft.core.error.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

object NoOp extends ReadCommand[Unit]

// 1) An in‐memory LogStorage
class InMemoryLogStorage[F[_]: Sync] extends LogStorage[F] {

  override def deleteBefore(index: Long): F[Unit] = ???

  private val ref = Ref.unsafe[F, Map[Long, LogEntry]](Map.empty)

  override def get(index: Long): F[Option[LogEntry]] =
    ref.get.map(_.get(index))

  override def put(index: Long, entry: LogEntry): F[LogEntry] =
    ref.update(_ + (index -> entry)).as(entry)

  override def deleteAfter(index: Long): F[Unit] =
    ref.update(_.filter { case (k, _) => k <= index })

  override def currentLength: F[Long] =
    ref.get.map(_.size.toLong)
}

class InMemoryStateMachine[F[_]: Sync] extends StateMachine[F] {
  // In‐memory state machine that does nothing

  override def applyWrite: PartialFunction[(Long, WriteCommand[?]), F[Any]] = { _ => Sync[F].pure(true) }

  override def applyRead: PartialFunction[ReadCommand[?], F[Any]] = { _ => Sync[F].pure(true) }

  override def appliedIndex: F[Long] = Sync[F].pure(0L)

}

class DummyMembershipManager[F[_]: Sync] extends MembershipManager[F]:

  val configurationRef: Ref[F, ClusterConfiguration] =
    Ref.unsafe[F, ClusterConfiguration](
      ClusterConfiguration(
        currentNode = Leader(currentTerm = 1L, address = NodeAddress("n1", 9090)),
        members = List(NodeAddress("n1", 9090), NodeAddress("n2", 9090), NodeAddress("n3", 9090))
      )
    )

  override def members: F[Set[Node]] = Monad[F].pure(
    Set(
      Leader(currentTerm = 1L, address = NodeAddress("n1", 9090)),
      Follower(currentTerm = 1L, address = NodeAddress("n2", 9090)),
      Follower(currentTerm = 1L, address = NodeAddress("n3", 9090))
    )
  )

  override def setClusterConfiguration(newConfig: ClusterConfiguration): F[Unit] = configurationRef.set(newConfig)
  override def getClusterConfiguration: F[ClusterConfiguration] = configurationRef.get

given logger: Logger[IO] = NoOpLogger[IO]
