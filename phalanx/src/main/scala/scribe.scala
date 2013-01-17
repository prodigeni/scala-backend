package com.wikia.phalanx

import com.twitter.finagle.Service
import com.twitter.util.{FuturePool, Future}
import com.wikia.wikifactory.ScribeLogger
import java.util.concurrent.Executors

abstract class ScribeLike extends Service[(String, Any), Unit] {
	def category(cat: String) = map( (obj:Any) => (cat, obj) )
}


class ScribeClient(host: String = "localhost", port:Int = 1463) extends ScribeLike {
	private val scribe = new ScribeLogger(host, port)
	private val threaded = FuturePool(Executors.newFixedThreadPool(1))
	// one thread to make sure we don't do concurent scribe requests

	override def release() {
		//todo: closing client if requierd?
	}

	def apply(request: (String, Any)): Future[Unit] = threaded {
		scribe.category(request._1)
		scribe.send(request._2)
	}
}

class ScribeBuffer extends ScribeLike {
	val requests = scala.collection.mutable.ArrayBuffer.empty[(String, Any)]
	def apply(request: (String, Any)): Future[Unit] = {
		requests += request
		Future.Done
	}
	def flushTo(other: ScribeLike): Future[Unit] = {
	  val result = requests.map { other(_) }
		requests.clear()
		Future.join(result)
	}
}