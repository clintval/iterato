package io.cvbio.collection

import java.io.Closeable
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference
import scala.collection.AbstractIterator
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, Future}

/** Helpers for parallel work over iterators. */
object ParIterator {

  /** The default duration to wait before cancelling computations running in threads. */
  private val DefaultTimeOut: Duration = 1.hour

  /** The available processors for this runtime, hyper-threading aware. */
  private[collection] lazy val AvailableProcessors = Runtime.getRuntime.availableProcessors()

  /** Parallelize work over an iterator using a collection of <threads> buffering <capacity> results at a time. An
    * additional single-thread execution context will be created to manage the side-effect of submitting all work
    * to the primary thread pool which means there is no condition under which this iterator will deadlock infinitely
    * unless you ask for infinite timeouts while awaiting computations (default is 1 hour). If <capacity> is set to
    * <None> then a dynamically expanding linked blocking queue is used but if <capacity> is set to a fixed size then
    * an array blocking queue is used. Any exception caused by the source <iterator> or by the applied function <fn>
    * will be raised. The underlying thread pool used by this method will be automatically shutdown upon iterator
    * exhaustion.
    *
    * @param iterator a stream of input items.
    * @param fn the function to apply over each item in the <iterator>.
    * @param threads perform work in a fixed thread pool with this many threads.
    * @param capacity the number of results to buffer at a time in the underlying blocking queue.
    * @param timeOut await each result this amount of time before cancelling the computation and raising an exception.
    */
  def map[A, B](
    iterator: Iterator[A],
    fn: A => B,
    threads: Int          = AvailableProcessors,
    capacity: Option[Int] = None,
    timeOut: Duration     = DefaultTimeOut,
  ): Iterator[B] = {
    val pool    = Executors.newFixedThreadPool(threads)
    val results = iterator.parMap(fn, capacity = capacity, timeOut = timeOut)(ExecutionContext.fromExecutorService(pool))
    new AbstractIterator[B] with Closeable { // Auto-close the wrapped thread pool on iterator exhaustion.
      private var open: Boolean = true
      override def hasNext: Boolean = if (open && results.hasNext) true else { close(); false }
      override def next(): B = {
        if (!open) Iterator.empty.next()
        val result = results.next()
        if (!hasNext) close()
        result
      }
      override def close(): Unit = if (open) { open = false; pool.shutdown() }
    }
  }

  /** Implicitly add parallel operations onto Scala's base iterator class. */
  implicit class ParIteratorImpl[A](private val iterator: Iterator[A]) {

    /** Parallelize work over an iterator using a given execution context buffering <capacity> results at a time. An
      * additional single-thread execution context will be created to manage the side-effect of submitting all work
      * to the primary <executor> which means there is no condition under which this iterator will deadlock infinitely
      * unless you ask for infinite timeouts while awaiting computations (default is 1 hour). If <capacity> is set to
      * <None> then a dynamically expanding linked blocking queue is used but if <capacity> is set to a fixed size then
      * an array blocking queue is used. Any exception caused by the source <iterator> or by the applied function <fn>
      * will be raised.
      *
      * @param fn the function to apply over each item in the <iterator>.
      * @param capacity the number of results to buffer at a time in the underlying blocking queue.
      * @param timeOut await each result this amount of time before cancelling the computation and raising an exception.
      */
    def parMap[B](fn: A => B, capacity: Option[Int] = None, timeOut: Duration = DefaultTimeOut)(
      implicit executor: ExecutionContext
    ): Iterator[B] = {
      val throwable = new AtomicReference[Throwable](null) // A place for any exceptions raised in the source iterator.
      val finished  = new CountDownLatch(1) // Set this to zero when we have finished sending jobs to the thread pool.
      val ioPool    = Executors.newSingleThreadExecutor
      val ioContext = ExecutionContext.fromExecutorService(ioPool)

      // Use a dynamically-expanding queue if no capacity was explicitly asked for, otherwise pre-allocate an array.
      val queue: BlockingQueue[Option[Future[B]]] = capacity match {
        case Some(size) => new ArrayBlockingQueue(size)
        case None       => new LinkedBlockingQueue()
      }

      // Use the IO execution context to fill the queue with results and terminate the queue with a final `None` to
      // indicate that the input iterator is fully exhausted and all Futures have been scheduled. We wrap this call in a
      // try-catch block in the exceptional case that exceptions are raised not in the input function, but in the source
      // iteration itself (`iterator.foreach(???)`)! Any exception will be saved, then the iterator will short-circuit.
      // Once the iterator short-circuits, an iterator exhaustion hook (defined below) will be called which includes a
      // method to raise the exception properly so it is not silenced. If we did not handle exceptions this way, then
      // the iterator could be truncated and data lost.
      Future {
        try { try iterator.foreach(elem => queue.put(Some(Future(fn(elem))(executor)))) finally queue.put(None) }
        catch { case thr: Throwable => throwable.compareAndSet(null, thr) }
        finally { finished.countDown() }
      }(ioContext)

      // Build the return iterator which will await results from the queue until the queue is empty.
      new Iterator[B] {

        /** Whether or not there is still pending work that is filling the queue with results. */
        private var alive: Boolean = true

        /** The next item in the queue as that item is pulled from this thread. */
        private var nextFuture: Option[Future[B]] = None

        /** If the iterator still has more items to yield. */
        override def hasNext: Boolean = {
          alive && {
            if (nextFuture.isEmpty) {
              nextFuture = queue.take() match {
                case None => alive = false; None
                case some => some
              }
            }
            // If there are no more Futures in the queue, then await the signal which indicates submission to the queue
            // has finished and any exceptions that were raised are saved to `throwable`. Once the queue is no longer
            // needed shutdown the queue to prevent a memory leak. Finally, Raise any exceptions that occurred during
            // source iteration so the exceptions are not silently dropped. It is critically important to call these
            // methods in this order because a race condition may occur when the source iterator raises an exception
            // and short-circuits, but we have not yet had a chance to save the exception message before finishing the
            // final call to `hasNext` (occurring in a separate thread). Awaiting the final countdown latch guarantees
            // we will raise the exception message if it is present.
            if (!alive) {
              finished.await()
              ioPool.shutdown()
              Option(throwable.get).foreach(throw _)
            }
            alive
          }
        }

        /** Return the next item in the iterator or raise an exception if there are no more item. */
        override def next(): B = {
          if (!hasNext) { Iterator.empty.next() } else {
            val value = Await.result(nextFuture.get, atMost = timeOut)
            nextFuture = None
            value
          }
        }
      }
    }
  }
}
