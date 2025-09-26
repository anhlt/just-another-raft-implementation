package com.grok.raft.core.examples

import cats.*
import cats.effect.IO
import cats.mtl.{Handle, Raise}
import cats.syntax.all.*
import com.grok.raft.core.error.{StateMachineError, LogError, RaftError}
import com.grok.raft.core.error.* // Import extension methods

/**
 * Example demonstrating MTL-style error handling patterns for the Raft implementation.
 * 
 * This shows how to migrate from MonadThrow-based error handling to MTL's typed error capabilities.
 */
object MtlExample {

  // Example 1: Simple operation with typed errors
  def parseInput[F[_]: Monad](input: String)(using Raise[F, StateMachineError]): F[Int] =
    if (input.forall(_.isDigit))
      input.toInt.pure[F]
    else
      StateMachineError.InvalidCommand(s"Non-numeric input: $input").raise[F, Int]

  // Example 2: Combining multiple error types in a single operation
  def processCommand[F[_]: Monad](cmdStr: String)(using 
    Raise[F, StateMachineError], 
    Raise[F, LogError]
  ): F[String] =
    for {
      value <- parseInput[F](cmdStr)
      result <- if (value > 0)
        s"Processed: $value".pure[F]
      else
        LogError.IndexOutOfBounds(value, 0).raise[F, String]
    } yield result

  // Example 3: Scoped error handling with allow/rescue
  def safeParsing(input: String): IO[String] = 
    Handle.allow[StateMachineError]:
      parseInput[IO](input).map(_.toString)
    .rescue:
      case StateMachineError.InvalidCommand(msg) =>
        IO.println(s"Parse failed: $msg") >> "0".pure[IO]
      case StateMachineError.OperationFailed(op, reason) =>
        IO.println(s"Operation $op failed: $reason") >> "0".pure[IO]
      case error =>
        IO.println(s"Unexpected error: $error") >> "0".pure[IO]

  // Example 4: Nested error handling with multiple scopes
  def complexOperation(inputs: List[String]): IO[List[Int]] =
    Handle.allow[RaftError]:
      inputs.traverse { input =>
        Handle.allow[StateMachineError]:
          parseInput[IO](input)
        .rescue:
          case StateMachineError.InvalidCommand(_) => 
            0.pure[IO] // Default value for invalid commands
          case error => 
            RaftError.InvalidTerm(0, -1).raise[IO, Int] // Escalate to higher error type
      }
    .rescue:
      case RaftError.InvalidTerm(current, request) =>
        IO.println(s"Term error: current=$current, request=$request") >> 
        List.empty[Int].pure[IO]
      case error =>
        IO.println(s"Raft error: $error") >> 
        List.empty[Int].pure[IO]

  // Example 5: Converting between error types during migration
  def legacyCompatibility[F[_]: MonadThrow](input: String): F[Int] =
    Handle.allow[StateMachineError]:
      parseInput[F](input)
    .rescue:
      case StateMachineError.InvalidCommand(msg) =>
        MonadThrow[F].raiseError(new IllegalArgumentException(msg))
      case StateMachineError.OperationFailed(op, reason) =>
        MonadThrow[F].raiseError(new RuntimeException(s"$op failed: $reason"))
      case error =>
        MonadThrow[F].raiseError(new Exception(error.toString))

  // Example 6: Testing with Either instead of IO
  def testWithEither(input: String): Either[StateMachineError, Int] =
    parseInput[Either[StateMachineError, *]](input)

  // Demo usage
  def demo(): IO[Unit] = 
    for {
      _ <- IO.println("=== MTL Error Handling Demo ===")
      
      // Simple parsing
      result1 <- safeParsing("123")
      _ <- IO.println(s"Parse '123': $result1")
      
      result2 <- safeParsing("abc")  
      _ <- IO.println(s"Parse 'abc': $result2")
      
      // Complex operation
      results <- complexOperation(List("1", "2", "invalid", "4"))
      _ <- IO.println(s"Complex operation results: $results")
      
      // Either-based testing
      testResult1 = testWithEither("42")
      testResult2 = testWithEither("invalid")
      _ <- IO.println(s"Either test valid: $testResult1")
      _ <- IO.println(s"Either test invalid: $testResult2")
      
    } yield ()

}