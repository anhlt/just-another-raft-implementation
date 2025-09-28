package com.grok.raft.effects.storage

import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.grok.raft.core.storage.{StateStorage, PersistedState}
import com.grok.raft.effects.storage.codec.RaftCodecs
import org.rocksdb.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*
import scala.jdk.CollectionConverters.*

class RocksDBStateStorage[F[_]: Async: Logger] private (
    val db: RocksDB,
    val stateCF: ColumnFamilyHandle,
    val writeOptions: WriteOptions
) extends StateStorage[F]:

  private val PERSISTED_STATE_KEY = "persisted_state".getBytes

  override def persistState(state: PersistedState): F[Unit] =
    for
      _ <- trace"Persisting Raft state: $state"
      encodedState <- RaftCodecs.encodePersistedState(state) match
        case Right(bytes) => bytes.pure[F]
        case Left(err) => 
          Sync[F].raiseError(new RuntimeException(s"Failed to encode persisted state: $err"))
      
      _ <- Sync[F].delay(db.put(stateCF, writeOptions, PERSISTED_STATE_KEY, encodedState))
      _ <- trace"Successfully persisted Raft state"
    yield ()

  override def retrieveState: F[Option[PersistedState]] =
    for
      _ <- trace"Retrieving persisted Raft state"
      bytes <- Sync[F].delay(db.get(stateCF, PERSISTED_STATE_KEY))
      result <- Option(bytes) match
        case None => 
          trace"No persisted state found" *> none[PersistedState].pure[F]
        case Some(data) =>
          RaftCodecs.decodePersistedState(data) match
            case Right(state) => 
              trace"Retrieved persisted state: $state" *> state.some.pure[F]
            case Left(err) => 
              error"Failed to decode persisted state: $err" *>
              Sync[F].raiseError(new RuntimeException(s"Failed to decode persisted state: $err"))
    yield result

object RocksDBStateStorage:

  def create[F[_]: Async: Logger](
      config: RocksDBConfig
  ): Resource[F, RocksDBStateStorage[F]] =
    
    val columnFamilyDescriptors = List(
      new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, config.createColumnFamilyOptions()),
      new ColumnFamilyDescriptor("raft_state".getBytes, config.createColumnFamilyOptions())
    )

    Resource.make {
      for
        _ <- info"Opening RocksDB state storage at ${config.stateMachineDbPath}/state"
        stateDbPath = config.stateMachineDbPath.resolve("state")
        _ <- Sync[F].delay(java.nio.file.Files.createDirectories(stateDbPath))
        
        columnFamilyHandles <- Sync[F].delay(new java.util.ArrayList[ColumnFamilyHandle]())
        db <- Sync[F].delay {
          RocksDB.open(
            config.createStateMachineDBOptions(),
            stateDbPath.toString,
            columnFamilyDescriptors.asJava,
            columnFamilyHandles
          )
        }
        
        defaultCF = columnFamilyHandles.get(0)
        stateCF = columnFamilyHandles.get(1)
        
        writeOptions <- Sync[F].delay {
          val opts = new WriteOptions()
          opts.setSync(true)
          opts
        }
        
        storage = new RocksDBStateStorage[F](db, stateCF, writeOptions)
        _ <- info"RocksDB state storage opened successfully"
      yield storage
    } { storage =>
      for
        _ <- info"Closing RocksDB state storage"
        _ <- Sync[F].delay(storage.writeOptions.close())
        _ <- Sync[F].delay(storage.stateCF.close())
        _ <- Sync[F].delay(storage.db.close())
        _ <- info"RocksDB state storage closed"
      yield ()
    }