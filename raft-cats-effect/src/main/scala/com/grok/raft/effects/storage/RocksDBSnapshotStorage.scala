package com.grok.raft.effects.storage

import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.grok.raft.core.storage.{SnapshotStorage, Snapshot}
import com.grok.raft.core.ClusterConfiguration
import org.rocksdb.*
import org.rocksdb.util.Environment
import java.nio.file.{Path, Files, StandardCopyOption}
import java.io.{File, FileInputStream, FileOutputStream, IOException}
import java.util.zip.{GZIPOutputStream, GZIPInputStream}
import java.nio.file.attribute.BasicFileAttributes
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax.*

case class RocksDBSnapshot(
    lastIndex: Long,
    checkpointPath: Path,
    config: ClusterConfiguration
)

case class CompressedSnapshot(
    lastIndex: Long,
    config: ClusterConfiguration,
    compressedData: Array[Byte]
)

class RocksDBSnapshotStorage[F[_]: Async: Logger] private (
    db: RocksDB,
    kvDataCF: ColumnFamilyHandle,
    config: RocksDBConfig
) extends SnapshotStorage[F, Map[Array[Byte], Array[Byte]]]:

  private def createTempSnapshotDir(lastIndex: Long): F[Path] =
    for
      tempDir <- Sync[F].delay {
        val temp = Files.createTempDirectory(s"raft_snapshot_${lastIndex}_")
        temp.toFile.deleteOnExit()
        temp
      }
    yield tempDir

  private def compressDirectory(sourceDir: Path): F[Array[Byte]] =
    for
      _ <- trace"Compressing checkpoint directory: $sourceDir"
      baos <- Sync[F].delay(new java.io.ByteArrayOutputStream())
      gzipOut <- Sync[F].delay(new GZIPOutputStream(baos))
      
      _ <- Sync[F].delay {
        def addFileToGzip(file: File, parentPath: String): Unit = {
          if file.isDirectory then
            file.listFiles().foreach(addFileToGzip(_, s"$parentPath${file.getName}/"))
          else
            val fis = new FileInputStream(file)
            val buffer = new Array[Byte](1024)
            val entryName = s"$parentPath${file.getName}"
            
            // Write file path length and path
            val pathBytes = entryName.getBytes("UTF-8")
            gzipOut.write((pathBytes.length >> 24).toByte)
            gzipOut.write((pathBytes.length >> 16).toByte)
            gzipOut.write((pathBytes.length >> 8).toByte)
            gzipOut.write(pathBytes.length.toByte)
            gzipOut.write(pathBytes)
            
            // Write file size
            val fileSize = file.length()
            gzipOut.write((fileSize >> 56).toByte)
            gzipOut.write((fileSize >> 48).toByte)
            gzipOut.write((fileSize >> 40).toByte)
            gzipOut.write((fileSize >> 32).toByte)
            gzipOut.write((fileSize >> 24).toByte)
            gzipOut.write((fileSize >> 16).toByte)
            gzipOut.write((fileSize >> 8).toByte)
            gzipOut.write(fileSize.toByte)
            
            // Write file content
            var bytesRead = fis.read(buffer)
            while bytesRead != -1 do
              gzipOut.write(buffer, 0, bytesRead)
              bytesRead = fis.read(buffer)
            
            fis.close()
        }
        
        addFileToGzip(sourceDir.toFile, "")
        gzipOut.close()
      }
      
      result = baos.toByteArray
      _ <- trace"Compressed directory to ${result.length} bytes"
    yield result

  private def decompressToDirectory(compressedData: Array[Byte], targetDir: Path): F[Unit] =
    for
      _ <- trace"Decompressing ${compressedData.length} bytes to directory: $targetDir"
      _ <- Sync[F].delay(Files.createDirectories(targetDir))
      
      bais <- Sync[F].delay(new java.io.ByteArrayInputStream(compressedData))
      gzipIn <- Sync[F].delay(new GZIPInputStream(bais))
      
      _ <- Sync[F].delay {
        val buffer = new Array[Byte](1024)
        
        def readInt(): Int = {
          (gzipIn.read() << 24) | (gzipIn.read() << 16) | (gzipIn.read() << 8) | gzipIn.read()
        }
        
        def readLong(): Long = {
          ((gzipIn.read().toLong << 56) | (gzipIn.read().toLong << 48) | 
           (gzipIn.read().toLong << 40) | (gzipIn.read().toLong << 32) |
           (gzipIn.read().toLong << 24) | (gzipIn.read().toLong << 16) | 
           (gzipIn.read().toLong << 8) | gzipIn.read().toLong)
        }
        
        while gzipIn.available() > 0 || gzipIn.read(buffer, 0, 1) > 0 do
          // Read file path length
          val pathLength = if gzipIn.available() > 0 then readInt() else buffer(0) << 24 | readInt()
          
          // Read file path
          val pathBytes = new Array[Byte](pathLength)
          gzipIn.readNBytes(pathBytes, 0, pathLength)
          val filePath = new String(pathBytes, "UTF-8")
          
          // Read file size
          val fileSize = readLong()
          
          // Create file and parent directories
          val targetFile = targetDir.resolve(filePath).toFile
          targetFile.getParentFile.mkdirs()
          
          // Write file content
          val fos = new FileOutputStream(targetFile)
          var remaining = fileSize
          while remaining > 0 do
            val toRead = math.min(buffer.length, remaining.toInt)
            val bytesRead = gzipIn.readNBytes(buffer, 0, toRead)
            if bytesRead > 0 then
              fos.write(buffer, 0, bytesRead)
              remaining -= bytesRead
          
          fos.close()
        
        gzipIn.close()
      }
      
      _ <- trace"Successfully decompressed to directory"
    yield ()

  private def deleteDirectory(dir: Path): F[Unit] =
    Sync[F].delay {
      def deleteRecursively(file: File): Unit = {
        if file.isDirectory then
          file.listFiles().foreach(deleteRecursively)
        file.delete(): Unit
      }
      deleteRecursively(dir.toFile)
    }

  override def persistSnapshot(snapshot: Snapshot[Map[Array[Byte], Array[Byte]]]): F[Unit] =
    // Snapshots are not persisted - they're created on-demand and sent over network
    trace"Snapshot persistence not implemented - snapshots are ephemeral"

  override def retrieveSnapshot: F[Option[Snapshot[Map[Array[Byte], Array[Byte]]]]] =
    // No persistent snapshot storage
    none[Snapshot[Map[Array[Byte], Array[Byte]]]].pure[F]

  override def getLatestSnapshot: F[Snapshot[Map[Array[Byte], Array[Byte]]]] =
    // Create snapshot on-demand from current state
    for
      _ <- trace"Creating on-demand snapshot from current state"
      currentState <- getCurrentState()
      // Note: ClusterConfiguration should come from Raft context
      snapshot = Snapshot(
        lastIndex = -1L, // Should be provided by caller
        data = currentState,
        config = ClusterConfiguration(null, List.empty) // Placeholder
      )
    yield snapshot

  def createRocksDBSnapshot(lastIndex: Long, clusterConfig: ClusterConfiguration): F[RocksDBSnapshot] =
    for
      _ <- trace"Creating RocksDB checkpoint snapshot at index $lastIndex"
      checkpointDir <- createTempSnapshotDir(lastIndex)
      // RocksDB checkpoint requires a non-existent directory, so delete it if it exists
      _ <- Sync[F].delay {
        if Files.exists(checkpointDir) then
          def deleteRecursively(file: java.io.File): Unit =
            if file.isDirectory then file.listFiles().foreach(deleteRecursively)
            file.delete()
          deleteRecursively(checkpointDir.toFile)
      }
      checkpoint <- Sync[F].delay(Checkpoint.create(db))
      _ <- Sync[F].delay(checkpoint.createCheckpoint(checkpointDir.toString))
      
      snapshot = RocksDBSnapshot(
        lastIndex = lastIndex,
        checkpointPath = checkpointDir,
        config = clusterConfig
      )
      _ <- trace"Successfully created RocksDB snapshot at $checkpointDir"
    yield snapshot

  def compressSnapshot(snapshot: RocksDBSnapshot): F[CompressedSnapshot] =
    for
      _ <- trace"Compressing RocksDB snapshot for transmission"
      compressedData <- compressDirectory(snapshot.checkpointPath)
      compressed = CompressedSnapshot(
        lastIndex = snapshot.lastIndex,
        config = snapshot.config,
        compressedData = compressedData
      )
      // Cleanup temporary checkpoint
      _ <- deleteDirectory(snapshot.checkpointPath)
      _ <- trace"Snapshot compressed and temporary files cleaned up"
    yield compressed

  def installCompressedSnapshot(
      compressedSnapshot: CompressedSnapshot,
      targetDbPath: Path
  ): F[Unit] =
    for
      _ <- trace"Installing compressed snapshot to $targetDbPath"
      tempDir <- createTempSnapshotDir(compressedSnapshot.lastIndex)
      
      // Decompress snapshot
      _ <- decompressToDirectory(compressedSnapshot.compressedData, tempDir)
      
      // Close current DB (caller responsibility)
      // Replace DB directory with snapshot
      _ <- Sync[F].delay {
        // Delete existing DB directory
        if Files.exists(targetDbPath) then
          deleteRecursively(targetDbPath.toFile)
        
        // Move decompressed snapshot to target location
        Files.move(tempDir, targetDbPath, StandardCopyOption.REPLACE_EXISTING)
      }
      
      _ <- trace"Successfully installed compressed snapshot"
    yield ()

  private def getCurrentState(): F[Map[Array[Byte], Array[Byte]]] =
    for
      iterator <- Sync[F].delay(db.newIterator(kvDataCF))
      result <- Sync[F].delay {
        iterator.seekToFirst()
        val state = scala.collection.mutable.Map[Array[Byte], Array[Byte]]()
        
        while iterator.isValid do
          state += ((iterator.key().clone(), iterator.value().clone()))
          iterator.next()
        
        iterator.close()
        state.toMap
      }
    yield result

  private def deleteRecursively(file: File): Unit = {
    if file.isDirectory then
      file.listFiles().foreach(deleteRecursively)
    file.delete(): Unit
  }

object RocksDBSnapshotStorage:

  def create[F[_]: Async: Logger](
      db: RocksDB,
      kvDataCF: ColumnFamilyHandle,
      config: RocksDBConfig
  ): F[RocksDBSnapshotStorage[F]] =
    for
      _ <- info"Creating RocksDB snapshot storage"
      storage = new RocksDBSnapshotStorage[F](db, kvDataCF, config)
      _ <- info"RocksDB snapshot storage created successfully"
    yield storage