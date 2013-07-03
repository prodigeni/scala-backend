package com.wikia.phalanx

import com.twitter.finagle.Service
import com.twitter.util._
import com.wikia.wikifactory.ScribeLogger
import java.util.concurrent.{TimeUnit, Executors}
import com.twitter.finagle.util.TimerFromNettyTimer
import org.jboss.netty.util.HashedWheelTimer
import collection.mutable


abstract class ScribeLike extends Service[Map[String, Any], Unit] {
	protected val logger = NiceLogger("Scribe")
	def many(request: Seq[Map[String, Any]]): Future[Unit]
}

class ScribeClient(category:String,host:String, port:Int) extends ScribeLike {
	private val scribe = new ScribeLogger(host, port)
	scribe.category(category)
	private val threaded = FuturePool(Executors.newFixedThreadPool(1))
	// one thread to make sure we don't do concurent scribe requests
	def apply(request: Map[String, Any]): Future[Unit] = threaded {
		logger.trace("Sending to scribe: "+request.toString)
		scribe.sendMaps(Seq(request))
	}
	def many(request: Seq[Map[String, Any]]): Future[Unit] = threaded {
		logger.trace("Sending to scribe: "+request.toString)
		scribe.sendMaps(request)
	}
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
		logger.trace("Flushing scribe buffer")
		val toFlush = requests.dequeueAll(_ => true)
		if (toFlush.nonEmpty) Await.result(other.many(toFlush.toSeq))
	}

	override def close(deadline: Time) = {
		timer.stop()
		other.close(deadline)
	}
}
class ScribeDiscard extends ScribeLike {
	// does nothing with requests
	def apply(request: Map[String, Any]): Future[Unit] = Future.Done
	def many(request: Seq[Map[String, Any]]): Future[Unit] = Future.Done
}