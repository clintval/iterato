package io.cvbio.collection

import io.cvbio.collection.ParIterator.{AvailableProcessors, ParIteratorImpl}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/** Unit tests for [[ParIterator]]. */
class ParIteratorTest extends AnyFlatSpec with Matchers {

  "ParIterator.map" should "return elements in the correct order when parallelized with a pre-allocated buffer capacity" in {
    val expected = Range(1, 1000).inclusive
    val actual   = ParIterator.map[Int, Int](
      expected.iterator,
      identity,
      threads  = AvailableProcessors,
      capacity = Some(100)
    ).toSeq
    actual should contain theSameElementsInOrderAs expected
  }

  if (AvailableProcessors > 1) { // To run these tests you must have more than one available processor.
    it should "raise exceptions that occur within passed function running in threads, but only if multiple threads are used" in {
      def raise(num: Int): Int = throw new IllegalArgumentException(num.toString)
      an[IllegalArgumentException] shouldBe thrownBy {
        ParIterator.map(
          iterator = Range(1, 10).iterator,
          fn       = raise,
          threads  = AvailableProcessors
        ).toSeq
      }
    }

    it should "raise exceptions that occur within the input iterator running in threads, but only if multiple threads are used" in {
      def raise(num: Int): Int = throw new IllegalArgumentException(num.toString)
      an[IllegalArgumentException] shouldBe thrownBy {
        ParIterator.map(
          iterator = Range(1, 10).iterator.map(raise),
          fn       = identity[Int],
          threads  = AvailableProcessors
        ).toSeq
      }
    }
  }

  "ParIterator.parMap" should "map over elements using a fixed size thread pool and a near-unlimited buffer" in {
    val pool    = Executors.newFixedThreadPool(AvailableProcessors)
    val context = ExecutionContext.fromExecutorService(pool)
    def addTen(int: Int): Int = int + 10
    val integers = Range(1, 10)
    val actual   = integers.iterator.parMap(addTen)(context).toSeq
    pool.shutdown()
    actual should contain theSameElementsInOrderAs integers.map(addTen)
  }

  it should "not deadlock if a fixed thread pool with one thread is requested" in {
    val pool    = Executors.newFixedThreadPool(1)
    val context = ExecutionContext.fromExecutorService(pool)
    def addTen(int: Int): Int = int + 10
    val integers = Range(1, 10)
    val actual   = integers.iterator.parMap(addTen)(context).toSeq
    pool.shutdown()
    actual should contain theSameElementsInOrderAs integers.map(addTen)
  }

  it should "map over elements using a fixed size thread pool and a buffer of a size smaller than the collection" in {
    val pool    = Executors.newFixedThreadPool(AvailableProcessors)
    val context = ExecutionContext.fromExecutorService(pool)
    def addTen(int: Int): Int = int + 10
    val integers = Range(1, 10)
    val actual   = integers.iterator.parMap(addTen, capacity = Some(1))(context).toSeq
    pool.shutdown()
    actual should contain theSameElementsInOrderAs integers.map(addTen)
  }

  it should "map over elements using the user-defined execution context and a right-sized buffer" in {
    val pool    = Executors.newFixedThreadPool(AvailableProcessors)
    val context = ExecutionContext.fromExecutorService(pool)
    def addTen(int: Int): Int = int + 10
    val integers = Range(1, 10)
    val actual   = integers.iterator.parMap(addTen, capacity = Some(integers.length))(context).toSeq
    pool.shutdown()
    actual should contain theSameElementsInOrderAs integers.map(addTen)
  }

  if (AvailableProcessors > 1) { // To run these tests you must have more than one available processor.
    it should "raise exceptions that occur within passed function running in threads, but only if multiple threads are used" in {
      val pool    = Executors.newFixedThreadPool(AvailableProcessors)
      val context = ExecutionContext.fromExecutorService(pool)
      def raise(num: Int): Int = throw new IllegalArgumentException(num.toString)
      an[IllegalArgumentException] shouldBe thrownBy { Range(1, 10).iterator.parMap(raise)(context).toSeq }
      pool.shutdown()
    }

    it should "raise exceptions that occur within the input iterator running in threads, but only if multiple threads are used" in {
      val pool    = Executors.newFixedThreadPool(AvailableProcessors)
      val context = ExecutionContext.fromExecutorService(pool)
      def raise(num: Int): Int = throw new IllegalArgumentException(num.toString)
      an[IllegalArgumentException] shouldBe thrownBy { Range(1, 10).iterator.map(raise).parMap(identity[Int])(context).toSeq }
      pool.shutdown()
    }
  }
}
