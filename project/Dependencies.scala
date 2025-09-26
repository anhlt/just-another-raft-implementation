import sbt._
import org.portablescala.sbtplatformdeps.PlatformDepsPlugin.autoImport._

object Dependencies {

  val catsCore         = Def.setting("org.typelevel" %% "cats-core" % "2.13.0")
  val catsMtl          = Def.setting("org.typelevel" %% "cats-mtl" % "1.6.0")
  val log4CatsCore     = Def.setting("org.typelevel" %% "log4cats-slf4j" % "2.7.1")
  val log4CatsLog4j    = Def.setting("org.typelevel" %% "log4cats-slf4j" % "2.7.1" % Test)
  val logback          = Def.setting("ch.qos.logback" % "logback-classic" % "1.5.18" % Test)
  val catsEffect       = Def.setting("org.typelevel" %% "cats-effect" % "3.6.3")
  val catsEffectKernel = Def.setting("org.typelevel" %% "cats-effect-kernel" % "3.6.3")
  val catsEffectStd    = Def.setting("org.typelevel" %% "cats-effect-std" % "3.6.3")

  val munit            = Def.setting("org.scalameta" %% "munit" % "1.1.0" % Test)
  val munitCatsEffect  = Def.setting("org.typelevel" %% "munit-cats-effect" % "2.1.0" % Test)
  val munitScalaCheck  = Def.setting("org.scalameta" %% "munit-scalacheck" % "1.2.0" % Test)
  val scalaCheck       = Def.setting("org.scalacheck" %% "scalacheck" % "1.16.0" % Test)
  val scalaCheckEffect = Def.setting("org.typelevel" %% "scalacheck-effect" % "2.0.0-M2" % Test)

  val munitScalaCheckEffect = Def.setting(
    "org.typelevel" %% "scalacheck-effect-munit" % "2.0.0-M2" % Test
  )
}
