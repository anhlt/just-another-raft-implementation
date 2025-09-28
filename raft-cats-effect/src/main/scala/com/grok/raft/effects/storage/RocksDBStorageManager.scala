package com.grok.raft.effects.storage

import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.mtl.Raise
import com.grok.raft.core.storage.{StateStorage, SnapshotStorage}
import com.grok.raft.core.internal.storage.LogStorage
import com.grok.raft.core.protocol.KVStateMachine
import com.grok.raft.core.error.StateMachineError
import com.grok.raft.core.ClusterConfiguration
import org.rocksdb.RocksDB
import java.nio.file.Path
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*

class RocksDBStorageManager[F[_]: Async: Logger] private (
    val logStorage: RocksDBLogStorage[F],
    val stateStorage: RocksDBStateStorage[F], 
    val kvStateMachine: RocksDBKVStateMachine[F],
    val snapshotStorage: RocksDBSnapshotStorage[F],
    config: RocksDBConfig
):

  def createSnapshot(
      lastIndex: Long, 
      clusterConfig: ClusterConfiguration
  ): F[CompressedSnapshot] =
    for
      _ <- info"Creating compressed snapshot at index $lastIndex"
      rocksDbSnapshot <- snapshotStorage.createRocksDBSnapshot(lastIndex, clusterConfig)
      compressedSnapshot <- snapshotStorage.compressSnapshot(rocksDbSnapshot)
      _ <- info"Successfully created compressed snapshot"
    yield compressedSnapshot

  def installSnapshot(
      compressedSnapshot: CompressedSnapshot
  ): F[Unit] =
    for
      _ <- info"Installing snapshot at index ${compressedSnapshot.lastIndex}"
      
      // Install snapshot to state machine DB
      kvDbPath = config.stateMachineDbPath.resolve("kv")
      _ <- snapshotStorage.installCompressedSnapshot(compressedSnapshot, kvDbPath)
      
      // Update log storage metadata after successful snapshot installation
      _ <- logStorage.deleteBefore(compressedSnapshot.lastIndex + 1)
      
      _ <- info"Successfully installed snapshot"
    yield ()

  def compactLog(snapshotIndex: Long): F[Unit] =
    logStorage.deleteBefore(snapshotIndex + 1)

  def close(): F[Unit] =
    for
      _ <- info"Closing RocksDB storage manager"
      // Resources are managed by individual storage components
      _ <- info"RocksDB storage manager closed"
    yield ()

object RocksDBStorageManager:

  def create[F[_]: Async: Logger](
      config: RocksDBConfig
  )(using Raise[F, StateMachineError]): Resource[F, RocksDBStorageManager[F]] =
    
    for
      _ <- Resource.eval(info"Creating RocksDB storage manager")
      
      // Initialize RocksDB native library
      _ <- Resource.eval(Sync[F].delay(RocksDB.loadLibrary()))
      
      // Create all storage components
      logStorage <- RocksDBLogStorage.create[F](config)
      stateStorage <- RocksDBStateStorage.create[F](config)
      kvStateMachine <- RocksDBKVStateMachine.create[F](config)
      
      // Create snapshot storage (needs access to KV state machine DB)
      snapshotStorage <- Resource.eval(
        RocksDBSnapshotStorage.create[F](
          kvStateMachine.db,
          kvStateMachine.kvDataCF,
          config
        )
      )
      
      manager = new RocksDBStorageManager[F](
        logStorage,
        stateStorage,
        kvStateMachine,
        snapshotStorage,
        config
      )
      
      _ <- Resource.eval(info"RocksDB storage manager created successfully")
      
    yield manager

  def createWithDefaultConfig[F[_]: Async: Logger](
      baseDir: Path
  )(using Raise[F, StateMachineError]): Resource[F, RocksDBStorageManager[F]] =
    create[F](RocksDBConfig.default(baseDir))