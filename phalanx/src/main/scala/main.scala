package com.wikia.phalanx

import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Http, RichHttp, Request, Status, Version, Response, Message}
import com.twitter.finagle.util.TimerFromNettyTimer
import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.util.{Time, TimerTask, FuturePool, Future}
import com.wikia.wikifactory._
import java.io.{FileInputStream, File}
import java.util.regex.PatternSyntaxException
import java.util.{Calendar, Date}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.jboss.netty.util.HashedWheelTimer
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConversions._
import util.parsing.json.{JSONArray, JSONFormat}
import com.newrelic.api.agent.{NewRelic, Trace}

class ExceptionLogger[Req,Rep](val logger: Logger) extends SimpleFilter[Req, Rep] {
	def this(loggerName: String) = this(LoggerFactory.getLogger(loggerName))
	def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
		service(request).onFailure( (exception) => {
			logger.error("Exception in service" , exception)
		})
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
                  val scribe:Service[Map[String, Any], Unit]) extends Service[Request, Response] {
	val logger = LoggerFactory.getLogger(classOf[MainService])
	val threaded = FuturePool.defaultPool
	var nextExpireDate: Option[Date] = None
	var expireWatchTask: Option[TimerTask] = None
	val timer = new TimerFromNettyTimer(new HashedWheelTimer())
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
	def afterReload(expired: Traversable[Int]): Future[Unit] = {
		threaded({
			reloader(rules, expired)
		}) map (newRules => {
			rules = newRules
			watchExpired()
		})
	}
	def selectRuleSystem(request: Request): Option[RuleSystem] = {
		request.params.get("type") match {
			case Some(s: String) => rules.get(s)
			case None => None
		}
	}
	@Trace
	def handleCheckOrMatch(func: (RuleSystem, Seq[Checkable]) => Response)(request: Request): Future[Response] = selectRuleSystem(request) match {
		case None => Future(Respond.error("Unknown type parameter"))
		case Some(ruleSystem: RuleSystem) => threaded {
			val params = request.getParams("content")
			if (params.isEmpty) Respond.error("content parameter is missing") else {
				val lang = request.params.getOrElse("lang", "en")
			  NewRelic.setTransactionName("phalanx", request.uri)
				val result = func(ruleSystem, params.map ( s => Checkable(s, lang ) ))
				result
			}
		}
	}

	val handleCheck = handleCheckOrMatch((ruleSystem, checkables) => {
		if (checkables.exists(c =>ruleSystem.isMatch(c))) {
			Respond.failure()
		} else {
			Respond.ok()
		}
	}) _
	val handleMatch = handleCheckOrMatch((ruleSystem, checkables) => {
		val matches = checkables.flatMap(c => ruleSystem.allMatches(c).map(r => r.dbId)).toSet.toSeq.sorted
		if (matches.isEmpty) {

		} else {

		}
		Respond.json(matches)
	}) _
	@Trace
	def validateRegex(request: Request) = {
		NewRelic.setTransactionName("phalanx", request.uri)
		val s = request.params.getOrElse("regex", "")
		try {
			s.r
			Respond.ok()
		} catch {
			case e: PatternSyntaxException => Respond.failure()
		}
	}
	def stats = {
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
			case Root / "check" => handleCheck(request)
			case Root / "match" => handleMatch(request)
			case Root / "validate" => threaded {
				validateRegex(request)
			}
			case Root / "reload" => {
				val ids = for (x <- request.getParam("changed", "").split(',')) yield x.toInt
				afterReload(ids).map(_ => Respond.ok())
			}
			case Root / "stats" => threaded {
				Respond(stats)
			}
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
		println("Loaded properties from "+fileName)
		properties
	}

	val cfName:Option[String] = sys.props.get("phalanx.config") orElse {
		// load config from first config file that exists: phalanx.properties, /etc/phalanx.properties, phalanx.default.properties
		Seq("./phalanx.properties", "/etc/phalanx.properties", "./phalanx.default.properties").find( fileName => {
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
	def wikiaProp(key:String) = sys.props("com.wikia.phalanx."+key)

	val logger = LoggerFactory.getLogger("Main")
	logger.trace("Properties loaded")
	val scribe = {
		val scribetype = wikiaProp("scribe")
		logger.info("Creating scribe client ("+scribetype+")")
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
	scribe(("test", Map(("somekey", "somevalue"))))() // make sure we're connected
	val port = wikiaProp("port").toInt

	logger.info("Loading rules from database")
	val db = new DB(DB.DB_MASTER, "", "wikicities").connect()
	val mainService = new MainService(RuleSystem.fromDatabase(db), (old, changed) => RuleSystem.reloadSome(db, old, changed.toSet),
		new ExceptionLogger(logger) andThen scribe.category("log_phalanx"))

	val config = ServerBuilder()
		.codec(RichHttp[Request](Http()))
		.name("Phalanx")
		.bindTo(new java.net.InetSocketAddress(port))

	val server = config.build(ExceptionFilter andThen mainService)
	logger.info("Server started on port: " + port)
	logger.trace("Initial stats: \n" + mainService.stats)
}

