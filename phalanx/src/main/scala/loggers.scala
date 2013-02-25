package com.wikia.phalanx

import org.slf4j.LoggerFactory
import com.twitter.util.Future
import com.twitter.util.Stopwatch

case class NiceLogger(name: String) {
	val logger = LoggerFactory.getLogger(name)
	def trace(messageBlock: => String) {
		if (logger.isTraceEnabled) logger.trace(messageBlock)
	}
	def debug(messageBlock: => String) {
		if (logger.isDebugEnabled) logger.debug(messageBlock)
	}
	def info(messageBlock: => String) {
		if (logger.isInfoEnabled) logger.info(messageBlock)
	}
	def warn(messageBlock: => String) {
		if (logger.isWarnEnabled) logger.warn(messageBlock)
	}
	def error(messageBlock: => String) {
		if (logger.isErrorEnabled) logger.error(messageBlock)
	}
	def exception(message: String, error: Throwable) {
		if (logger.isErrorEnabled) logger.error(message, error)
	}
	def timeIt[T](name:String)(func: => T):T = {
		if (logger.isTraceEnabled) {
			val elapsed = Stopwatch.start()
			val result = func
			val duration = elapsed()
			logger.trace(s"$name ${duration.inMillis}ms")
			result
		} else func
	}
	def timeIt[T](name:String, future: => Future[T]):Future[T] = {
		if (logger.isTraceEnabled) {
			val elapsed = Stopwatch.start()
			future.onSuccess( _ => {
				val duration = elapsed()
				logger.trace(s"$name ${duration.inMillis}ms")
			})
		} else future
	}

	lazy val functions = Map( ("info", info _), ("error", error _), ("debug", debug _), ("warn", warn _), ("trace", trace _) )
	def apply(level:String, messageBlock: => String) = functions.get(level).map { _(messageBlock) }
}
