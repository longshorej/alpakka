/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.mqtt.streaming.impl

import akka.actor.typed.{Behavior, Signal}
import akka.actor.typed.scaladsl.{Behaviors, StashBuffer}
import akka.stream.QueueOfferResult

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

private[mqtt] object QueueOfferState {

  /**
   * A marker trait that holds a result for SourceQueue#offer
   */
  trait QueueOfferCompleted {
    def result: Either[Throwable, QueueOfferResult]
  }

  /**
   * A behavior that stashes messages until a response to the SourceQueue#offer
   * method is received.
   *
   * This is to be used only with SourceQueues that use backpressure.
   */
  def waitForQueueOfferCompleted[T](result: Future[QueueOfferResult],
                                    f: Try[QueueOfferResult] => T,
                                    behavior: Behavior[T],
                                    stash: StashBuffer[T]): Behavior[T] = Behaviors.setup { context =>
    import context.executionContext

    if (result.isCompleted) {
      // optimize for a common case where we were immediately able to enqueue

      result.value.get match {
        case Success(QueueOfferResult.Enqueued) =>
          stash.unstashAll(context, behavior)

        case Success(other) =>
          throw new IllegalStateException(s"Failed to offer to queue: $other")

        case Failure(failure) =>
          throw failure
      }
    } else {
      result.onComplete { r =>
        context.self.tell(f(r))
      }

      val s = StashBuffer[Either[Signal, T]](Int.MaxValue)

      stash.foreach(m => s.stash(Right(m)))

      behaviorImpl(behavior, s)
    }
  }

  private def behaviorImpl[T](behavior: Behavior[T], stash: StashBuffer[Either[Signal, T]]): Behavior[T] =
    Behaviors
      .receive[T] {
        case (context, completed: QueueOfferCompleted) =>
          completed.result match {
            case Right(QueueOfferResult.Enqueued) =>
              var b = Behavior.start(behavior, context)

              stash.foreach {
                case Right(msg) =>
                  val nextBehavior = Behavior.interpretMessage(b, context, msg)

                  if (nextBehavior ne Behavior.same) {
                    b = nextBehavior
                  }

                case Left(signal) =>
                  val nextBehavior = Behavior.interpretSignal(b, context, signal)

                  if (nextBehavior ne Behavior.same) {
                    b = nextBehavior
                  }

              }

              b

            case Right(other) =>
              throw new IllegalStateException(s"Failed to offer to queue: $other")

            case Left(failure) =>
              throw failure
          }

        case (_, other) =>
          stash.stash(Right(other))

          Behavior.same
      }
      .receiveSignal {
        case (_, signal) =>
          stash.stash(Left(signal))

          Behavior.same
      }
}
