package com.grok.raft.core

import cats.effect.IOApp
import cats.effect.IO

object Main extends IOApp.Simple {

  // This is your new "main"!
  def run: IO[Unit] = IO.pure("Hello Wolf").as(println("Hello Wolf"))
}
