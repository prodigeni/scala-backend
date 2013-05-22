// helpers for NewRelic
// universal, could be moved to wikifactory and used in other finagle based application

package com.wikia.phalanx

import com.newrelic.api.agent
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.Duration
import org.slf4j.LoggerFactory
import collection.convert.WrapAsJava

case class NewRelicReq(name:String, req:Request) extends agent.Request {
	val empty = collection.JavaConversions.asJavaEnumeration(Seq.empty[String].toIterator)
	def getRequestURI = name
	def getHeader(name: String) = req.headers.getOrElse(name, null)
	def getRemoteUser = null
	def getParameterNames = WrapAsJava.asJavaEnumeration(req.params.keys.toIterator)
	def getParameterValues(name: String) = req.params.getAll(name).toArray
	def getAttribute(name: String) = null
}
sealed abstract class NewRelicResponse extends agent.Response {
	def setHeader(p1: String, p2: String) {}
}
case class NewRelicOK(response: Response) extends NewRelicResponse {
	def getStatus = 200
	def getStatusMessage = response.contentString
}
case class NewRelicError(e:Throwable) extends NewRelicResponse {
	def getStatus = 500
	def getStatusMessage = e.getClass.getName
}


abstract class NewRelicHttpFilter extends SimpleFilter[Request, Response] {
	val logger = LoggerFactory.getLogger("NewRelic")
	def classifier(request: Request):Option[String]
	def timeString(duration: Duration) = duration.inMillis.toString + "ms"
	def apply(request: Request, service: Service[Request, Response]) = {
    val elapsed = com.twitter.util.Stopwatch.start()
		classifier(request) match {
      case Some(name) => {
        val nReq = NewRelicReq(name, request)
        val future = service(request)
        future.onSuccess { response =>
          val duration = elapsed()
          logger.trace(name + " " + timeString(duration))
          reportOk(nReq, duration, NewRelicOK(response))
        }
          .onFailure { e =>
          val duration = elapsed()
          logger.warn(name + " " + timeString(duration), e)
          reportErr(nReq, duration, e)
        }
      }
      case _ => service(request) // don't include other requests in NewRelic stats
    }
	}
	@agent.Trace(dispatcher=true)
	def reportOk(req:NewRelicReq, duration:Duration, resp: NewRelicResponse) {
		agent.NewRelic.setTransactionName(null, req.name)
		agent.NewRelic.setRequestAndResponse(req, resp)
		agent.NewRelic.recordResponseTimeMetric(req.name, duration.inMillis)
	}
	@agent.Trace(dispatcher=true)
	def reportErr(req:NewRelicReq, duration:Duration, e:Throwable) {
		agent.NewRelic.setTransactionName(null, req.name)
		agent.NewRelic.setRequestAndResponse(req, NewRelicError(e))
		agent.NewRelic.noticeError(e)
		agent.NewRelic.recordResponseTimeMetric(req.name, duration.inMillis)
	}
}

object NewRelic extends NewRelicHttpFilter {
	def classifier(request: Request):Option[String] = {
    val allTypes = request.params.getAll("type")
    if (allTypes.isEmpty) None else Some(allTypes.toSeq.sorted.mkString(","))
	}
}
