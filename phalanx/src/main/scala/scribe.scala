package com.wikia.phalanx

import com.twitter.finagle.Service
import com.twitter.util.{Duration, FuturePool, Future}
import com.wikia.wikifactory.ScribeLogger
import java.util.concurrent.{TimeUnit, Executors}
import com.twitter.finagle.util.TimerFromNettyTimer
import org.jboss.netty.util.HashedWheelTimer
import collection.mutable

abstract class ScribeLike extends Service[Map[String, Any], Unit] {
	def many(request: Seq[Map[String, Any]]): Future[Unit]
}

class ScribeClient(category:String,host:String, port:Int) extends ScribeLike {
	private val scribe = new ScribeLogger(host, port)
	scribe.category(category)
	private val threaded = FuturePool(Executors.newFixedThreadPool(1))
	// one thread to make sure we don't do concurent scribe requests
	def apply(request: Map[String, Any]): Future[Unit] = threaded { scribe.sendMaps(Seq(request)) }
	def many(request: Seq[Map[String, Any]]): Future[Unit] = threaded { scribe.sendMaps(request) }
}

class ScribeBuffer(val other: ScribeLike, val duration: Duration) extends ScribeLike {
	val timer = new TimerFromNettyTimer(new HashedWheelTimer(duration.inNanoseconds, TimeUnit.NANOSECONDS))
	val requests = new mutable.SynchronizedQueue[Map[String, Any]]
	def apply(request: Map[String, Any]): Future[Unit] = {
		requests += request
		timer.doLater(duration) { flush() }
	}
	def many(request: Seq[Map[String, Any]]): Future[Unit] = {
		requests ++= request
		timer.doLater(duration) { flush() }
	}
	private def flush() {
		val toFlush = requests.dequeueAll(_ => true)
		if (toFlush.nonEmpty) other.many(toFlush.toSeq)()
	}

	override def release() {
		timer.stop()
		other.release()
	}
}
class ScribeDiscard extends ScribeLike {
	// does nothing with requests
	def apply(request: Map[String, Any]): Future[Unit] = Future.Done
	def many(request: Seq[Map[String, Any]]): Future[Unit] = Future.Done
}