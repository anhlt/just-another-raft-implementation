import org.typelevel.sbt.tpolecat.*

inThisBuild(
  Seq(
    scalaVersion               := "3.7.0",
    organization               := "com.grok.raft",
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
    Dependencies.log4CatsCore.value,
    Dependencies.log4CatsLog4j.value,
    Dependencies.logback.value,

    // test
    Dependencies.munitCatsEffect.value,
    Dependencies.munit.value,
    Dependencies.scalaCheck.value,
    Dependencies.munitScalaCheck.value,
    Dependencies.munitScalaCheckEffect.value
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

val raftCatsEffect = (project in file("raft-cats-effect"))
  .settings(
    name := "raft-cats-effect",
    commonSettings
  )
  .dependsOn(raft)

lazy val docs = project // new documentation project
  .in(file("just-another-raft-docs")) // important: it must not be docs/
  .dependsOn(raft)
  .settings(
    moduleName    := "just-another-raft-docs",
    mdocVariables := Map("VERSION" -> version.value)
  )
  .enablePlugins(MdocPlugin, DocusaurusPlugin)

val root = (project in file("."))
  .settings(
    name := "Just Another Raft Implementation"
  )
  .aggregate(raft)
