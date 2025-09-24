package com.grok.raft.core.storage

import cats.effect.*
import cats.syntax.all.*
import com.grok.raft.core.internal.NodeAddress
import java.nio.file.Path
import java.nio.ByteBuffer

class RocksDBStateStorage[F[_]: Sync] private (
    private val rocksDB: RocksDBWrapper[F]
) extends StateStorage[F] {

  private val STATE_CF = "state"
  private val PERSISTED_STATE_KEY = "persisted_state".getBytes("UTF-8")

  override def persistState(state: PersistedState): F[Unit] =
    for {
      serialized <- Sync[F].pure(serializeState(state))
      _ <- rocksDB.put(PERSISTED_STATE_KEY, serialized, STATE_CF)
    } yield ()

  override def retrieveState: F[Option[PersistedState]] =
    rocksDB.get(PERSISTED_STATE_KEY, STATE_CF).map {
      case Some(bytes) => Some(deserializeState(bytes))
      case None => None
    }

  private def serializeState(state: PersistedState): Array[Byte] = {
    val buffer = ByteBuffer.allocate(512)
    buffer.putLong(state.term)
    buffer.putLong(state.appliedIndex)
    
    state.votedFor match {
      case Some(nodeAddr) =>
        buffer.put(1.toByte) // Has votedFor
        val ipBytes = nodeAddr.ip.getBytes("UTF-8")
        buffer.putInt(ipBytes.length)
        buffer.put(ipBytes)
        buffer.putInt(nodeAddr.port)
      case None =>
        buffer.put(0.toByte) // No votedFor
    }
    
    val result = new Array[Byte](buffer.position())
    buffer.flip()
    buffer.get(result)
    result
  }

  private def deserializeState(bytes: Array[Byte]): PersistedState = {
    val buffer = ByteBuffer.wrap(bytes)
    val term = buffer.getLong()
    val appliedIndex = buffer.getLong()
    val hasVotedFor = buffer.get()
    
    val votedFor = if (hasVotedFor == 1.toByte) {
      val ipLength = buffer.getInt()
      val ipBytes = new Array[Byte](ipLength)
      buffer.get(ipBytes)
      val ip = new String(ipBytes, "UTF-8")
      val port = buffer.getInt()
      Some(NodeAddress(ip, port))
    } else {
      None
    }
    
    PersistedState(term, votedFor, appliedIndex)
  }
}

object RocksDBStateStorage {
  
  def apply[F[_]: Sync](path: Path): Resource[F, RocksDBStateStorage[F]] =
    RocksDBWrapper[F](path).map(new RocksDBStateStorage[F](_))
}