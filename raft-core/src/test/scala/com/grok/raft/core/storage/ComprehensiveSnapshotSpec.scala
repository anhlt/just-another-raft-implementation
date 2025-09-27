package com.grok.raft.core.storage

import cats.*
import cats.effect.*
import cats.effect.implicits.*
import cats.implicits.*
import cats.syntax.all.*
import cats.mtl.Raise
import com.grok.raft.core.*
import com.grok.raft.core.internal.*
import com.grok.raft.core.internal.given
import com.grok.raft.core.storage.Snapshot
import com.grok.raft.core.protocol.*
import com.grok.raft.core.error.StateMachineError
import munit.CatsEffectSuite

class ComprehensiveSnapshotSpec extends CatsEffectSuite {

  import MtlTestUtils.*
  import MtlTestUtils.given
  import TestData.*

  val testConfig = ClusterConfiguration(
    currentNode = leader,
    members = List(addr1, addr2, addr3)
  )

  // ===========================
  // Custom State Machines
  // ===========================

  // Counter State Machine for testing arithmetic operations
  case class CounterState(value: Long, operations: List[String])

  case class Increment(amount: Long) extends WriteCommand[Long]
  case class Decrement(amount: Long) extends WriteCommand[Long]
  case class GetValue() extends ReadCommand[Long]
  case class GetHistory() extends ReadCommand[List[String]]
  case class Reset() extends WriteCommand[Unit]

  class CounterStateMachine[F[_]](implicit syncF: Sync[F], raiseF: Raise[F, StateMachineError]) 
    extends StateMachine[F, CounterState] {
    
    private val stateRef = Ref.unsafe[F, CounterState](CounterState(0L, List.empty))
    private val indexRef = Ref.unsafe[F, Long](0L)

    override def applyWrite: PartialFunction[(Long, WriteCommand[?]), F[Any]] = {
      case (index, Increment(amount)) =>
        for {
          _ <- indexRef.set(index)
          _ <- stateRef.update(state => state.copy(
            value = state.value + amount,
            operations = s"increment($amount)" :: state.operations
          ))
          newState <- stateRef.get
        } yield newState.value.asInstanceOf[Any]

      case (index, Decrement(amount)) =>
        for {
          _ <- indexRef.set(index)
          _ <- stateRef.update(state => state.copy(
            value = state.value - amount,
            operations = s"decrement($amount)" :: state.operations
          ))
          newState <- stateRef.get
        } yield newState.value.asInstanceOf[Any]

      case (index, Reset()) =>
        for {
          _ <- indexRef.set(index)
          _ <- stateRef.set(CounterState(0L, "reset" :: Nil))
        } yield ().asInstanceOf[Any]
    }

    override def applyRead: PartialFunction[ReadCommand[?], F[Any]] = {
      case GetValue() =>
        stateRef.get.map(_.value.asInstanceOf[Any])

      case GetHistory() =>
        stateRef.get.map(_.operations.reverse.asInstanceOf[Any])
    }

    override def appliedIndex: F[Long] = indexRef.get

    override def restoreSnapshot[T](lastIndex: Long, data: T): F[Unit] = {
      try {
        val counterState = data.asInstanceOf[CounterState]
        stateRef.set(counterState) *> indexRef.set(lastIndex)
      } catch {
        case _: ClassCastException => 
          raiseF.raise(StateMachineError.StateCorruption("Invalid counter state data"))
        case e: Exception => 
          raiseF.raise(StateMachineError.OperationFailed("restoreSnapshot", e.getMessage))
      }
    }

    override def getCurrentState: F[CounterState] = stateRef.get
  }

  // Bank Account State Machine for testing transactional operations
  case class Account(id: String, balance: Double)
  case class BankState(accounts: Map[String, Account], transactions: List[String])

  case class CreateAccount(id: String, initialBalance: Double) extends WriteCommand[Unit]
  case class Deposit(accountId: String, amount: Double) extends WriteCommand[Double]
  case class Withdraw(accountId: String, amount: Double) extends WriteCommand[Double]
  case class Transfer(fromId: String, toId: String, amount: Double) extends WriteCommand[Boolean]
  case class GetBalance(accountId: String) extends ReadCommand[Option[Double]]
  case class GetAccount(accountId: String) extends ReadCommand[Option[Account]]
  case class ListAccounts() extends ReadCommand[List[Account]]
  case class GetTransactionHistory() extends ReadCommand[List[String]]

  class BankStateMachine[F[_]](implicit syncF: Sync[F], raiseF: Raise[F, StateMachineError]) 
    extends StateMachine[F, BankState] {
    
    private val stateRef = Ref.unsafe[F, BankState](BankState(Map.empty, List.empty))
    private val indexRef = Ref.unsafe[F, Long](0L)

    override def applyWrite: PartialFunction[(Long, WriteCommand[?]), F[Any]] = {
      case (index, CreateAccount(id, initialBalance)) =>
        if (initialBalance < 0) {
          operationFailed("CreateAccount", "Initial balance cannot be negative")
        } else {
          for {
            _ <- indexRef.set(index)
            state <- stateRef.get
            _ <- if (state.accounts.contains(id)) {
              operationFailed("CreateAccount", s"Account $id already exists")
            } else {
              val newAccount = Account(id, initialBalance)
              val newState = state.copy(
                accounts = state.accounts + (id -> newAccount),
                transactions = s"create_account($id, $initialBalance)" :: state.transactions
              )
              stateRef.set(newState)
            }
          } yield ().asInstanceOf[Any]
        }

      case (index, Deposit(accountId, amount)) =>
        if (amount <= 0) {
          operationFailed("Deposit", "Amount must be positive")
        } else {
          for {
            _ <- indexRef.set(index)
            state <- stateRef.get
            result <- state.accounts.get(accountId) match {
              case Some(account) =>
                val updatedAccount = account.copy(balance = account.balance + amount)
                val newState = state.copy(
                  accounts = state.accounts + (accountId -> updatedAccount),
                  transactions = s"deposit($accountId, $amount)" :: state.transactions
                )
                stateRef.set(newState) *> Sync[F].pure(updatedAccount.balance.asInstanceOf[Any])
              case None =>
                operationFailed("Deposit", s"Account $accountId not found")
            }
          } yield result
        }

      case (index, Withdraw(accountId, amount)) =>
        if (amount <= 0) {
          operationFailed("Withdraw", "Amount must be positive")
        } else {
          for {
            _ <- indexRef.set(index)
            state <- stateRef.get
            result <- state.accounts.get(accountId) match {
              case Some(account) if account.balance >= amount =>
                val updatedAccount = account.copy(balance = account.balance - amount)
                val newState = state.copy(
                  accounts = state.accounts + (accountId -> updatedAccount),
                  transactions = s"withdraw($accountId, $amount)" :: state.transactions
                )
                stateRef.set(newState) *> Sync[F].pure(updatedAccount.balance.asInstanceOf[Any])
              case Some(_) =>
                operationFailed("Withdraw", "Insufficient funds")
              case None =>
                operationFailed("Withdraw", s"Account $accountId not found")
            }
          } yield result
        }

      case (index, Transfer(fromId, toId, amount)) =>
        if (amount <= 0) {
          operationFailed("Transfer", "Amount must be positive")
        } else {
          for {
            _ <- indexRef.set(index)
            state <- stateRef.get
            result <- (state.accounts.get(fromId), state.accounts.get(toId)) match {
              case (Some(fromAccount), Some(toAccount)) if fromAccount.balance >= amount =>
                val updatedFromAccount = fromAccount.copy(balance = fromAccount.balance - amount)
                val updatedToAccount = toAccount.copy(balance = toAccount.balance + amount)
                val newState = state.copy(
                  accounts = state.accounts + (fromId -> updatedFromAccount) + (toId -> updatedToAccount),
                  transactions = s"transfer($fromId -> $toId, $amount)" :: state.transactions
                )
                stateRef.set(newState) *> Sync[F].pure(true.asInstanceOf[Any])
              case (Some(_), Some(_)) =>
                operationFailed("Transfer", "Insufficient funds in source account")
              case (None, _) =>
                operationFailed("Transfer", s"Source account $fromId not found")
              case (_, None) =>
                operationFailed("Transfer", s"Destination account $toId not found")
            }
          } yield result
        }
    }

    override def applyRead: PartialFunction[ReadCommand[?], F[Any]] = {
      case GetBalance(accountId) =>
        stateRef.get.map(_.accounts.get(accountId).map(_.balance).asInstanceOf[Any])

      case GetAccount(accountId) =>
        stateRef.get.map(_.accounts.get(accountId).asInstanceOf[Any])

      case ListAccounts() =>
        stateRef.get.map(_.accounts.values.toList.asInstanceOf[Any])

      case GetTransactionHistory() =>
        stateRef.get.map(_.transactions.reverse.asInstanceOf[Any])
    }

    override def appliedIndex: F[Long] = indexRef.get

    override def restoreSnapshot[T](lastIndex: Long, data: T): F[Unit] = {
      try {
        val bankState = data.asInstanceOf[BankState]
        stateRef.set(bankState) *> indexRef.set(lastIndex)
      } catch {
        case _: ClassCastException => 
          raiseF.raise(StateMachineError.StateCorruption("Invalid bank state data"))
        case e: Exception => 
          raiseF.raise(StateMachineError.OperationFailed("restoreSnapshot", e.getMessage))
      }
    }

    override def getCurrentState: F[BankState] = stateRef.get
  }

  // Custom Log implementation with state machines
  class CustomLog[F[_]: Sync, T](val customStateMachine: StateMachine[F, T]) extends Log[F, T] {
    override val logStorage = new InMemoryLogStorage[F]
    override val snapshotStorage = new InMemorySnapshotStorage[F, T]
    override val membershipManager = new DummyMembershipManager[F]
    override val stateMachine = customStateMachine

    override def transactional[A](t: => F[A]): F[A] = t

    private val commitRef = Ref.unsafe[F, Long](-1L)
    override def getCommittedIndex: F[Long] = commitRef.get
    override def setCommitIndex(i: Long): F[Unit] = commitRef.set(i)

    override def state: F[LogState] = Sync[F].pure(LogState(0, None, 0))
  }

  // ===========================
  // Comprehensive Tests
  // ===========================

  test("Counter state machine read/write operations with snapshots") {
    withStateMachineErrorHandling {
      for {
        counterSM <- IO(new CounterStateMachine[IO])
        log <- IO(new CustomLog[IO, CounterState](counterSM))
        
        // Perform write operations
        _ <- log.logStorage.put(1, LogEntry(1, 1, Increment(5)))
        _ <- log.stateMachine.applyWrite((1, Increment(5)))
        
        _ <- log.logStorage.put(2, LogEntry(1, 2, Increment(3)))
        _ <- log.stateMachine.applyWrite((2, Increment(3)))
        
        _ <- log.logStorage.put(3, LogEntry(1, 3, Decrement(2)))
        _ <- log.stateMachine.applyWrite((3, Decrement(2)))
        
        // Read current state
        currentValue <- log.stateMachine.applyRead(GetValue()).map(_.asInstanceOf[Long])
        history <- log.stateMachine.applyRead(GetHistory()).map(_.asInstanceOf[List[String]])
        
        // Create snapshot
        _ <- log.createSnapshot(3)
        
        // Reset state machine
        _ <- log.stateMachine.applyWrite((4, Reset()))
        resetValue <- log.stateMachine.applyRead(GetValue()).map(_.asInstanceOf[Long])
        
        // Restore from snapshot
        snapshot <- log.snapshotStorage.retrieveSnapshot
        _ <- log.stateMachine.restoreSnapshot(snapshot.get.lastIndex, snapshot.get.data)
        
        // Verify restored state
        restoredValue <- log.stateMachine.applyRead(GetValue()).map(_.asInstanceOf[Long])
        restoredHistory <- log.stateMachine.applyRead(GetHistory()).map(_.asInstanceOf[List[String]])
        appliedIndex <- log.stateMachine.appliedIndex
        
      } yield {
        assertEquals(currentValue, 6L)
        assertEquals(history, List("increment(5)", "increment(3)", "decrement(2)"))
        assertEquals(resetValue, 0L)
        assertEquals(restoredValue, 6L) 
        assertEquals(restoredHistory, List("increment(5)", "increment(3)", "decrement(2)"))
        assertEquals(appliedIndex, 3L)
      }
    }
  }

  test("Bank state machine complex read/write operations with snapshots") {
    withStateMachineErrorHandling {
      for {
        bankSM <- IO(new BankStateMachine[IO])
        log <- IO(new CustomLog[IO, BankState](bankSM))
        
        // Create accounts
        _ <- log.stateMachine.applyWrite((1, CreateAccount("alice", 1000.0)))
        _ <- log.stateMachine.applyWrite((2, CreateAccount("bob", 500.0)))
        _ <- log.stateMachine.applyWrite((3, CreateAccount("charlie", 250.0)))
        
        // Read account states
        aliceBalance <- log.stateMachine.applyRead(GetBalance("alice")).map(_.asInstanceOf[Option[Double]])
        bobAccount <- log.stateMachine.applyRead(GetAccount("bob")).map(_.asInstanceOf[Option[Account]])
        allAccounts <- log.stateMachine.applyRead(ListAccounts()).map(_.asInstanceOf[List[Account]])
        
        // Perform transactions
        _ <- log.stateMachine.applyWrite((4, Deposit("alice", 200.0)))
        newAliceBalance <- log.stateMachine.applyWrite((5, Withdraw("alice", 150.0))).map(_.asInstanceOf[Double])
        transferSuccess <- log.stateMachine.applyWrite((6, Transfer("bob", "charlie", 100.0))).map(_.asInstanceOf[Boolean])
        
        // Read updated state
        finalAliceBalance <- log.stateMachine.applyRead(GetBalance("alice")).map(_.asInstanceOf[Option[Double]])
        finalBobBalance <- log.stateMachine.applyRead(GetBalance("bob")).map(_.asInstanceOf[Option[Double]])
        finalCharlieBalance <- log.stateMachine.applyRead(GetBalance("charlie")).map(_.asInstanceOf[Option[Double]])
        transactions <- log.stateMachine.applyRead(GetTransactionHistory()).map(_.asInstanceOf[List[String]])
        
        // Create snapshot with complex state
        _ <- log.createSnapshot(6)
        snapshot <- log.snapshotStorage.retrieveSnapshot
        
        // Simulate system restart by creating new state machine
        newBankSM <- IO(new BankStateMachine[IO])
        newLog <- IO(new CustomLog[IO, BankState](newBankSM))
        
        // Restore from snapshot
        _ <- newLog.stateMachine.restoreSnapshot(snapshot.get.lastIndex, snapshot.get.data)
        
        // Verify restored state
        restoredAliceBalance <- newLog.stateMachine.applyRead(GetBalance("alice")).map(_.asInstanceOf[Option[Double]])
        restoredBobBalance <- newLog.stateMachine.applyRead(GetBalance("bob")).map(_.asInstanceOf[Option[Double]])
        restoredCharlieBalance <- newLog.stateMachine.applyRead(GetBalance("charlie")).map(_.asInstanceOf[Option[Double]])
        restoredTransactions <- newLog.stateMachine.applyRead(GetTransactionHistory()).map(_.asInstanceOf[List[String]])
        restoredAppliedIndex <- newLog.stateMachine.appliedIndex
        
      } yield {
        // Initial state assertions
        assertEquals(aliceBalance, Some(1000.0))
        assertEquals(bobAccount, Some(Account("bob", 500.0)))
        assertEquals(allAccounts.size, 3)
        
        // Transaction result assertions
        assertEquals(newAliceBalance, 1050.0) // 1000 + 200 - 150
        assertEquals(transferSuccess, true)
        assertEquals(finalAliceBalance, Some(1050.0))
        assertEquals(finalBobBalance, Some(400.0)) // 500 - 100
        assertEquals(finalCharlieBalance, Some(350.0)) // 250 + 100
        assertEquals(transactions.size, 6)
        
        // Snapshot restoration assertions
        assertEquals(restoredAliceBalance, Some(1050.0))
        assertEquals(restoredBobBalance, Some(400.0))
        assertEquals(restoredCharlieBalance, Some(350.0))
        assertEquals(restoredTransactions.size, 6)
        assertEquals(restoredAppliedIndex, 6L)
      }
    }
  }

  test("Enhanced KV state machine with comprehensive read/write operations") {
    withStateMachineErrorHandling {
      for {
        kvSM <- IO(new InMemoryKVStateMachine[IO])
        log <- IO(new CustomLog[IO, Map[Array[Byte], Array[Byte]]](kvSM))
        
        // Test data
        key1 = "user:1001".getBytes
        value1 = """{"name":"Alice","age":30}""".getBytes
        key2 = "user:1002".getBytes  
        value2 = """{"name":"Bob","age":25}""".getBytes
        key3 = "product:2001".getBytes
        value3 = """{"title":"Laptop","price":999.99}""".getBytes
        key4 = "user:1003".getBytes
        value4 = """{"name":"Charlie","age":35}""".getBytes
        
        // Write operations
        _ <- kvSM.applyWrite((1, Put(key1, value1)))
        _ <- kvSM.applyWrite((2, Put(key2, value2)))
        _ <- kvSM.applyWrite((3, Put(key3, value3)))
        _ <- kvSM.applyWrite((4, Put(key4, value4)))
        
        // Read operations
        alice <- kvSM.applyRead(Get(key1)).map(_.asInstanceOf[Option[Array[Byte]]])
        bobExists <- kvSM.applyRead(Contains(key2)).map(_.asInstanceOf[Boolean])
        
        // Range operations
        userPrefix = "user:".getBytes
        userKeys <- kvSM.applyRead(Keys(Some(userPrefix), Some(10))).map(_.asInstanceOf[List[Array[Byte]]])
        userEntries <- kvSM.applyRead(Scan(userPrefix, Some(5))).map(_.asInstanceOf[List[(Array[Byte], Array[Byte])]])
        
        // Range query between user:1001 and user:1003
        startKey = "user:1001".getBytes
        endKey = "user:1003".getBytes
        rangeResults <- kvSM.applyRead(Range(startKey, endKey, None)).map(_.asInstanceOf[List[(Array[Byte], Array[Byte])]])
        
        // Create snapshot
        currentState <- kvSM.getCurrentState
        snapshot = Snapshot(4L, currentState, testConfig)
        _ <- log.snapshotStorage.persistSnapshot(snapshot)
        
        // Modify state after snapshot
        _ <- kvSM.applyWrite((5, Delete(key2)))
        _ <- kvSM.applyWrite((6, Put("temp:key".getBytes, "temp:value".getBytes)))
        
        modifiedState <- kvSM.getCurrentState
        modifiedSize = modifiedState.size
        
        // Restore from snapshot
        retrievedSnapshot <- log.snapshotStorage.retrieveSnapshot
        _ <- kvSM.restoreSnapshot(retrievedSnapshot.get.lastIndex, retrievedSnapshot.get.data)
        
        // Verify restored state
        restoredAlice <- kvSM.applyRead(Get(key1)).map(_.asInstanceOf[Option[Array[Byte]]])
        restoredBobExists <- kvSM.applyRead(Contains(key2)).map(_.asInstanceOf[Boolean])
        restoredState <- kvSM.getCurrentState
        restoredAppliedIndex <- kvSM.appliedIndex
        
      } yield {
        // Write/Read operation assertions
        assert(alice.isDefined)
        assert(new String(alice.get).contains("Alice"))
        assertEquals(bobExists, true)
        
        // Range operation assertions
        assertEquals(userKeys.size, 3) // user:1001, user:1002, user:1003
        assertEquals(userEntries.size, 3)
        assert(userEntries.forall { case (k, v) => new String(k).startsWith("user:") })
        
        // Range query assertions
        assertEquals(rangeResults.size, 2) // user:1001 and user:1002 (exclusive end)
        
        // State modifications
        assertEquals(modifiedSize, 4) // 4 original - 1 deleted + 1 new
        
        // Snapshot restoration assertions
        assert(restoredAlice.isDefined)
        assertEquals(restoredBobExists, true) // Bob should be restored
        assertEquals(restoredState.size, 4) // Original 4 entries restored
        assertEquals(restoredAppliedIndex, 4L)
        assert(!restoredState.contains("temp:key".getBytes)) // Temp entry should not exist
      }
    }
  }

  test("State machine error handling during read/write operations") {
    withStateMachineErrorHandling {
      for {
        bankSM <- IO(new BankStateMachine[IO])
        log <- IO(new CustomLog[IO, BankState](bankSM))
        
        // Test error conditions
        createNegativeResult <- bankSM.applyWrite((1, CreateAccount("test", -100.0))).attempt
        
        withdrawFromNonexistentResult <- bankSM.applyWrite((2, Withdraw("nonexistent", 50.0))).attempt
        
        // Create valid account for further tests
        _ <- bankSM.applyWrite((3, CreateAccount("test", 100.0)))
        
        overdraftResult <- bankSM.applyWrite((4, Withdraw("test", 200.0))).attempt
        
        invalidDepositResult <- bankSM.applyWrite((5, Deposit("test", -50.0))).attempt
        
        duplicateAccountResult <- bankSM.applyWrite((6, CreateAccount("test", 200.0))).attempt
        
        // Test successful operations after errors
        validDeposit <- bankSM.applyWrite((7, Deposit("test", 25.0))).map(_.asInstanceOf[Double])
        finalBalance <- bankSM.applyRead(GetBalance("test")).map(_.asInstanceOf[Option[Double]])
        
      } yield {
        // Error assertions
        assert(createNegativeResult.isLeft)
        assert(withdrawFromNonexistentResult.isLeft)
        assert(overdraftResult.isLeft)
        assert(invalidDepositResult.isLeft)
        assert(duplicateAccountResult.isLeft)
        
        // Valid operations assertions
        assertEquals(validDeposit, 125.0)
        assertEquals(finalBalance, Some(125.0))
      }
    }
  }

  test("Concurrent read operations should be safe") {
    withStateMachineErrorHandling {
      for {
        kvSM <- IO(new InMemoryKVStateMachine[IO])
        
        // Setup initial data
        keys = (1 to 100).map(i => s"key:$i".getBytes).toList
        values = (1 to 100).map(i => s"value:$i".getBytes).toList
        
        // Write all data sequentially
        _ <- keys.zip(values).zipWithIndex.toList.traverse { case ((k, v), i) =>
          kvSM.applyWrite((i.toLong + 1, Put(k, v)))
        }
        
        // Concurrent read operations
        readOps = keys.take(20).map { k =>
          kvSM.applyRead(Get(k)).map(_.asInstanceOf[Option[Array[Byte]]])
        }
        
        results <- readOps.parSequence
        
        // Concurrent range operations
        rangeOps = (1 to 10).map { i =>
          val start = s"key:${i * 5}".getBytes
          val end = s"key:${i * 5 + 3}".getBytes  
          kvSM.applyRead(Range(start, end, Some(2))).map(_.asInstanceOf[List[(Array[Byte], Array[Byte])]])
        }.toList
        
        rangeResults <- rangeOps.parSequence
        
      } yield {
        // All reads should succeed
        assert(results.forall(_.isDefined))
        assertEquals(results.size, 20)
        
        // Range operations should return results
        assert(rangeResults.forall(_.nonEmpty))
        assertEquals(rangeResults.size, 10)
      }
    }
  }

  test("Large state machine snapshot and restoration") {
    withStateMachineErrorHandling {
      for {
        kvSM <- IO(new InMemoryKVStateMachine[IO])
        log <- IO(new CustomLog[IO, Map[Array[Byte], Array[Byte]]](kvSM))
        
        // Generate large dataset
        largeData = (1 to 1000).map { i =>
          val key = f"large_key_$i%04d".getBytes
          val value = s"large_value_$i:${"x" * 100}".getBytes // ~120 bytes per value
          (key, value)
        }
        
        // Write large dataset
        _ <- largeData.zipWithIndex.toList.traverse { case ((k, v), i) =>
          kvSM.applyWrite((i.toLong + 1, Put(k, v)))
        }
        
        // Verify some entries
        firstEntry <- kvSM.applyRead(Get("large_key_0001".getBytes)).map(_.asInstanceOf[Option[Array[Byte]]])
        lastEntry <- kvSM.applyRead(Get("large_key_1000".getBytes)).map(_.asInstanceOf[Option[Array[Byte]]])
        
        // Create snapshot of large state
        largeState <- kvSM.getCurrentState
        largeSnapshot = Snapshot(1000L, largeState, testConfig)
        _ <- log.snapshotStorage.persistSnapshot(largeSnapshot)
        
        // Clear state
        _ <- largeData.indices.toList.traverse { i =>
          kvSM.applyWrite((i.toLong + 1001, Delete(largeData(i)._1)))
        }
        
        clearedState <- kvSM.getCurrentState
        
        // Restore large snapshot
        retrievedSnapshot <- log.snapshotStorage.retrieveSnapshot
        _ <- kvSM.restoreSnapshot(retrievedSnapshot.get.lastIndex, retrievedSnapshot.get.data)
        
        // Verify restoration
        restoredFirstEntry <- kvSM.applyRead(Get("large_key_0001".getBytes)).map(_.asInstanceOf[Option[Array[Byte]]])
        restoredLastEntry <- kvSM.applyRead(Get("large_key_1000".getBytes)).map(_.asInstanceOf[Option[Array[Byte]]])
        restoredState <- kvSM.getCurrentState
        restoredAppliedIndex <- kvSM.appliedIndex
        
        // Test range queries on restored large dataset
        prefixResults <- kvSM.applyRead(Scan("large_key_01".getBytes, Some(5))).map(_.asInstanceOf[List[(Array[Byte], Array[Byte])]])
        
      } yield {
        // Original state assertions
        assert(firstEntry.isDefined)
        assert(lastEntry.isDefined)
        assert(new String(firstEntry.get).contains("large_value_1"))
        assert(new String(lastEntry.get).contains("large_value_1000"))
        
        // Cleared state assertion
        assertEquals(clearedState.size, 0)
        
        // Restored state assertions
        assert(restoredFirstEntry.isDefined)
        assert(restoredLastEntry.isDefined)
        assertEquals(restoredState.size, 1000)
        assertEquals(restoredAppliedIndex, 1000L)
        assertEquals(prefixResults.size, 5)
        
        // Verify data integrity in restored state
        assert(new String(restoredFirstEntry.get).contains("large_value_1"))
        assert(new String(restoredLastEntry.get).contains("large_value_1000"))
      }
    }
  }

  test("Mixed read/write operations with multiple snapshots") {
    withStateMachineErrorHandling {
      for {
        bankSM <- IO(new BankStateMachine[IO])
        log <- IO(new CustomLog[IO, BankState](bankSM))
        
        // Initial setup
        _ <- bankSM.applyWrite((1, CreateAccount("savings", 1000.0)))
        _ <- bankSM.applyWrite((2, CreateAccount("checking", 500.0)))
        
        // First snapshot
        _ <- log.createSnapshot(2)
        snapshot1 <- log.snapshotStorage.retrieveSnapshot
        
        // More operations
        _ <- bankSM.applyWrite((3, Deposit("savings", 200.0)))
        balance1 <- bankSM.applyRead(GetBalance("savings")).map(_.asInstanceOf[Option[Double]])
        
        _ <- bankSM.applyWrite((4, Transfer("checking", "savings", 100.0)))
        
        // Second snapshot
        _ <- log.createSnapshot(4)
        snapshot2 <- log.snapshotStorage.retrieveSnapshot
        
        // Final operations
        _ <- bankSM.applyWrite((5, Withdraw("savings", 50.0)))
        finalSavingsBalance <- bankSM.applyRead(GetBalance("savings")).map(_.asInstanceOf[Option[Double]])
        finalCheckingBalance <- bankSM.applyRead(GetBalance("checking")).map(_.asInstanceOf[Option[Double]])
        
        transactions <- bankSM.applyRead(GetTransactionHistory()).map(_.asInstanceOf[List[String]])
        
        // Test restoration from first snapshot
        newBankSM1 <- IO(new BankStateMachine[IO])
        _ <- newBankSM1.restoreSnapshot(snapshot1.get.lastIndex, snapshot1.get.data)
        restoredBalance1 <- newBankSM1.applyRead(GetBalance("savings")).map(_.asInstanceOf[Option[Double]])
        
        // Test restoration from second snapshot
        newBankSM2 <- IO(new BankStateMachine[IO])
        _ <- newBankSM2.restoreSnapshot(snapshot2.get.lastIndex, snapshot2.get.data)
        restoredBalance2 <- newBankSM2.applyRead(GetBalance("savings")).map(_.asInstanceOf[Option[Double]])
        restoredChecking2 <- newBankSM2.applyRead(GetBalance("checking")).map(_.asInstanceOf[Option[Double]])
        
      } yield {
        // Operation flow assertions
        assertEquals(balance1, Some(1200.0)) // 1000 + 200
        assertEquals(finalSavingsBalance, Some(1250.0)) // 1200 + 100 - 50
        assertEquals(finalCheckingBalance, Some(400.0)) // 500 - 100
        assertEquals(transactions.size, 5)
        
        // Snapshot 1 restoration (after account creation only)
        assertEquals(restoredBalance1, Some(1000.0))
        
        // Snapshot 2 restoration (after deposit and transfer)
        assertEquals(restoredBalance2, Some(1300.0)) // 1000 + 200 + 100
        assertEquals(restoredChecking2, Some(400.0)) // 500 - 100
      }
    }
  }
}