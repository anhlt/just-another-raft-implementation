import org.typelevel.sbt.tpolecat.*

inThisBuild(
  Seq(
    scalaVersion := "3.4.0",
    organization := "com.grok.raft",
    tpolecatDefaultOptionsMode := VerboseMode
  )
)

val commonSettings = Seq(
  libraryDependencies ++= Seq(
    // "core" module - IO, IOApp, schedulers
    // This pulls in the kernel and std modules automatically.
    Dependencies.catsEffect.value,
    // concurrency abstractions and primitives (Concurrent, Sync, Async etc.)
    Dependencies.catsEffectKernel.value,
    // standard "effect" library (Queues, Console, Random etc.)
    Dependencies.catsEffectStd.value,
    Dependencies.munitCatsEffect.value
  )
)

val raftGrpc = (project in file("raft-grpc"))
  .settings(
    name := "raft-grpc",
    commonSettings
  )
  .enablePlugins(Fs2Grpc)

val raft = (project in file("raft-core"))
  .settings(
    name := "raft-core",
    commonSettings
  )
  .dependsOn(raftGrpc)



val root = (project in file("."))
  .settings(
    name := "Just Another Raft Implementation"
  )
  .aggregate(raft)
