package com.wikia.phalanx

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.RichHttp
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.http.path./
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Http, Request, Status, Version, Response, Message}
import com.twitter.finagle.util.TimerFromNettyTimer
import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.util._
import com.wikia.wikifactory._
import java.io.{FileInputStream, File}
import java.util.regex.PatternSyntaxException
import java.util.{Calendar, Date}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.jboss.netty.util.HashedWheelTimer
import scala.Some
import scala.collection.JavaConversions._
import util.parsing.json.JSONArray
import util.parsing.json.JSONFormat
import org.slf4j.LoggerFactory

class ExceptionLogger[Req, Rep](val logger: NiceLogger) extends SimpleFilter[Req, Rep] {
	def this(loggerName: String) = this(NiceLogger(loggerName))
	def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
		service(request).onFailure((exception) => {
			logger.error("Exception in service", exception)
		})
	}
}

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
	def error(message: String, error: Throwable) {
		if (logger.isErrorEnabled) logger.error(message, error)
	}
	def timeIt[T](name:String)(func: => T):T = {
		if (logger.isTraceEnabled) {
			val start = Time.now
			val result = func
			val duration = Time.now - start
			logger.trace(name+" "+ duration.inMillis+"ms")
			result
		} else func
	}
	def timeIt[T](name:String, future: => Future[T]):Future[T] = {
		if (logger.isTraceEnabled) {
			val start = Time.now
			future.onSuccess( _ => {
				val duration = Time.now - start
				logger.trace(name+" "+ duration.inMillis+"ms")
			})
		} else future
	}
}


object Respond {
	def apply(content: String, status: HttpResponseStatus = Status.Ok, contentType: String = "text/plain; charset=utf-8") = {
		val response = Response(Version.Http11, status)
		response.contentType = contentType
		response.contentString = content
		response
	}
	def json(data: Traversable[Int]) = Respond(JSONArray(data.toList).toString(JSONFormat.defaultFormatter), Status.Ok, Message.ContentTypeJson)
	def error(info: String, status: HttpResponseStatus = Status.InternalServerError) = Respond(info + "\n", status)
	def ok(s: String = "") = Respond("ok\n" + s)
	def failure(s: String = "") = Respond("failure\n" + s)
}

class MainService(var rules: Map[String, RuleSystem], val reloader: (Map[String, RuleSystem], Traversable[Int]) => Map[String, RuleSystem],
                  val scribe: Service[Map[String, Any], Unit]) extends Service[Request, Response] {
	val logger = NiceLogger("MainService")
	var nextExpireDate: Option[Date] = None
	var expireWatchTask: Option[TimerTask] = None
	val timer = new TimerFromNettyTimer(new HashedWheelTimer())
	/*
	val futurePool = FuturePool(
		java.util.concurrent.Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors(), new NamedPoolThreadFactory("MainService pool"))
	)
	*/
	val futurePool = FuturePool.immediatePool   // we use netty I/O workers thread anyway...

	watchExpired()

	override def release() {
		logger.trace("Stopping expired timer")
		timer.stop()
		logger.trace("Stopping scribe client")
		scribe.release()
	}
	def watchExpired() {
		val minDates = rules.values.flatMap(ruleSystem => ruleSystem.expiring.headOption.map(rule => rule.expires.get))
		expireWatchTask.map(task => {
			logger.trace("Old expire task " + task + "cancelled")
			task.cancel()
		})
		if (minDates.isEmpty) {
			nextExpireDate = None
			expireWatchTask = None
			logger.trace("Expire task not required")
		} else {
			val c = java.util.Calendar.getInstance(DB.dbTimeZone)
			c.setTime(minDates.min)
			c.set(Calendar.SECOND, 0)
			c.add(Calendar.MINUTE, 1)
			nextExpireDate = Some(c.getTime)
			expireWatchTask = Some(timer.schedule(Time(nextExpireDate.get)) {
				expire()
			})
			logger.trace("Scheduling expire task at " + nextExpireDate.get)
		}
	}
	def expire() {
		// todo: somehow run this on main finagle thread?
		val now = new Date().getTime
		val expired = rules.values.flatMap(ruleSystem => ruleSystem.expiring.takeWhile(rule => rule.expires.get.getTime <= now)).map(r => r.dbId)
		afterReload(expired)
	}
	def afterReload(expired: Traversable[Int]) {
		rules = reloader(rules, expired)
		watchExpired()
	}
	def handleCheckOrMatch(request: Request, func: (RuleSystem, Seq[Checkable]) => Response): Response = {
		logger.timeIt("handleCheckOrMatch") {
			val checkType = logger.timeIt("request.params") { request.params.get("type") }
			val ruleSystem = logger.timeIt("checkType") { checkType match {
				case Some(s: String) => {
					rules.get(s)
				}
				case None => None
			} }
			logger.timeIt("ruleSystem match") { ruleSystem match {
				case None => Respond.error("Unknown type parameter")
				case Some(ruleSystem: RuleSystem) => {
					val params = request.getParams("content")
					if (params.isEmpty) Respond.error("content parameter is missing")
					else {
						val lang = request.params.getOrElse("lang", "en")
						func(ruleSystem, params.map(s => Checkable(s, lang)))
					}
				}
			} }
			}
	}
	def handleCheck(request: Request) = {
		handleCheckOrMatch(request, (ruleSystem, checkables) => {
			logger.timeIt("handleCheck") {
			if (checkables.exists(c => ruleSystem.isMatch(c))) {
				Respond.failure()
			} else {
				Respond.ok()
			}
			}
		})
	}
	def handleMatch(request: Request) = {
		handleCheckOrMatch(request, (ruleSystem, checkables) => {
			logger.timeIt("handleMatch") {
			val matches = checkables.flatMap(c => ruleSystem.allMatches(c).map(r => r.dbId)).toSet.toSeq.sorted
			Respond.json(matches)
			}
		})
	}
	def validateRegex(request: Request) = {
		val s = request.params.getOrElse("regex", "")
		val response = try {
			s.r
			Respond.ok()
		} catch {
			case e: PatternSyntaxException => Respond.failure()
		}
		response
	}
	def reload(request: Request) = {
		val changed = request.getParam("changed", "").split(',').toSeq.filter {
			_ != ""
		}
		val ids = if (changed.isEmpty) Seq.empty[Int]
		else changed.map {
			_.toInt
		}
		afterReload(ids)
		Respond.ok()
	}
	def stats(request: Request): Response = {
		Respond(statsString)
	}
	def statsString: String = {
		val response = (rules.toSeq.map(t => {
			val (s, ruleSystem) = t
			s + ":\n" + (ruleSystem.stats.map {
				"  " + _
			}.mkString("\n")) + "\n"
		}) ++ Seq("Next rule expire date: " + nextExpireDate)).mkString("\n")
		response
	}
	def apply(request: Request): Future[Response] = {
		Path(request.path) match {
			case Root => Future(Respond("PHALANX ALIVE"))
			case Root / "check" => futurePool(handleCheck(request))
			case Root / "match" => futurePool(handleMatch(request))
			case Root / "validate" => futurePool(validateRegex(request))
			case Root / "reload" => futurePool(reload(request))
			case Root / "stats" => futurePool(stats(request))
			case _ => Future(Respond.error("not found", Status.NotFound))
		}
	}
}

class Jsvc {
	/*
	void init(String[] arguments): Here open configuration files, create a trace file, create ServerSockets, Threads
	void start(): Start the Thread, accept incoming connections
	void stop(): Inform the Thread to terminate the run(), close the ServerSockets
	void destroy(): Destroy any object created in init()
  */
	def init(arguments: Array[String]) {

	}
	def start() {

	}
	def stop() {

	}
	def destroy() {

	}
}

object Main extends App {
	def loadProperties(fileName: String): java.util.Properties = {
		val file = new File(fileName)
		val properties = new java.util.Properties()
		properties.load(new FileInputStream(file))
		println("Loaded properties from " + fileName)
		properties
	}

	val cfName: Option[String] = sys.props.get("phalanx.config") orElse {
		// load config from first config file that exists: phalanx.properties, /etc/phalanx.properties, phalanx.default.properties
		Seq("./phalanx.properties", "/etc/phalanx.properties", "./phalanx.default.properties").find(fileName => {
			val file = new File(fileName)
			file.exists() && file.canRead
		})
	}
	cfName match {
		case Some(fileName) => sys.props ++= loadProperties(fileName).toMap
		case None => {
			println("Don't know where to load configuration from.")
			System.exit(2)
		}
	}
	def wikiaProp(key: String) = sys.props("com.wikia.phalanx." + key)

	val logger = NiceLogger("Main")
	logger.trace("Properties loaded")
	val scribe = {
		val scribetype = wikiaProp("scribe")
		logger.info("Creating scribe client (" + scribetype + ")")
		scribetype match {
			case "send" => {
				val host = wikiaProp("scribe.host")
				val port = wikiaProp("scribe.port").toInt
				new ScribeClient(host, port)
			}
			case "buffer" => new ScribeBuffer()
			case "discard" => new ScribeDiscard()
		}
	}
	scribe(("test", Map(("somekey", "somevalue"))))()
	// make sure we're connected
	val port = wikiaProp("port").toInt

	logger.info("Loading rules from database")
	val db = new DB(DB.DB_MASTER, "", "wikicities").connect()
	val mainService = new MainService(RuleSystem.fromDatabase(db), (old, changed) => RuleSystem.reloadSome(db, old, changed.toSet),
		new ExceptionLogger(logger) andThen scribe.category("log_phalanx"))

	val config = ServerBuilder()
		.codec(RichHttp[Request](Http()))
		.name("Phalanx")
		.maxConcurrentRequests(20)
		.sendBufferSize(1024)
		.recvBufferSize(1024)
		.backlog(100)
		.bindTo(new java.net.InetSocketAddress(port))

	val server = config.build(ExceptionFilter andThen NewRelic andThen mainService)
	logger.info("Server started on port: " + port)
	logger.trace("Initial stats: \n" + mainService.statsString)
}

