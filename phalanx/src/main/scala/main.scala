package com.wikia.phalanx

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.RichHttp
import com.twitter.finagle.http.filter.ExceptionFilter
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
import java.util.concurrent.TimeUnit

class ExceptionLogger[Req, Rep](val logger: NiceLogger) extends SimpleFilter[Req, Rep] {
	def this(loggerName: String) = this(NiceLogger(loggerName))
	def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
		service(request).onFailure((exception) => {
			logger.error("Exception in service", exception)
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
	def json(data: Iterable[DatabaseRuleInfo]) = {
		val jsonData = JSONArray(data.toList.map(x => x.toJSONObject))
		Respond(jsonData.toString(JSONFormat.defaultFormatter), Status.Ok, Message.ContentTypeJson)
	}
	def error(info: String, status: HttpResponseStatus = Status.InternalServerError) = Respond(info + "\n", status)
	def ok(s: String = "") = Respond("ok\n" + s)
	def failure(s: String = "") = Respond("failure\n" + s)
}

class MainService(val reloader: (Map[String, RuleSystem], Traversable[Int]) => Map[String, RuleSystem],
                  val scribe: Service[Map[String, Any], Unit]) extends Service[Request, Response] {
	def this(initialRules: Map[String, RuleSystem], scribe: Service[Map[String, Any], Unit]) = this( (_, _) => initialRules, scribe)
	val logger = NiceLogger("MainService")
	var nextExpireDate: Option[Date] = None
	var expireWatchTask: Option[TimerTask] = None
	val timer = new TimerFromNettyTimer(new HashedWheelTimer(1, TimeUnit.SECONDS))
	var rules = reloader(Map.empty, Seq.empty)

	val threadPoolSize = Runtime.getRuntime.availableProcessors()
	//val threadPoolSize = 0

	val futurePool  = if (threadPoolSize == 0) FuturePool.immediatePool else FuturePool(
		java.util.concurrent.Executors.newFixedThreadPool(threadPoolSize, new NamedPoolThreadFactory("MainService pool"))
	)

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
		rules = reloader(rules, expired).toMap
		watchExpired()
	}
	def handleCheckOrMatch(request: Request, func: (RuleSystem, Iterable[Checkable]) => Response): Response = {
		val params =  request.params
		val checkType = params.get("type")
		val ruleSystem = checkType match {
			case Some(s: String) =>  rules.get(s)
			case None => None
		}
		ruleSystem match {
			case None => Respond.error("Unknown type parameter")
			case Some(ruleSystem: RuleSystem) => {
				val content = params.getAll("content")
				if (content.isEmpty) Respond.error("content parameter is missing")
				else {
					val lang = params.getOrElse("lang", "en")
					func(ruleSystem, content.map(s => Checkable(s, lang)))
				}
			}
		}
	}
	def handleCheck(request: Request) = {
		handleCheckOrMatch(request, (ruleSystem, checkables) => {
			if (checkables.exists(c => ruleSystem.isMatch(c))) {
				Respond.failure()
			} else {
				Respond.ok()
			}
		})
	}
	def handleMatch(request: Request) = {
		handleCheckOrMatch(request, (ruleSystem, checkables) => {
			val limit = request.params.getIntOrElse("limit", 1)
			val matches = checkables.toStream.flatMap(c => ruleSystem.allMatches(c)).take(limit)
			Respond.json(matches)
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
	val mainService = new MainService((old, changed) => RuleSystem.reloadSome(db, old, changed.toSet),
		new ExceptionLogger(logger) andThen scribe.category("log_phalanx"))

	val config = ServerBuilder()
		.codec(RichHttp[Request](Http()))
		.name("Phalanx")
		.maxConcurrentRequests(Seq(20, mainService.threadPoolSize).max)
		.sendBufferSize(16*1024)
		.recvBufferSize(16*1024)
		.backlog(100)
		.bindTo(new java.net.InetSocketAddress(port))

	val server = config.build(ExceptionFilter andThen NewRelic andThen mainService)
	logger.info("Server started on port: " + port)
	logger.trace("Initial stats: \n" + mainService.statsString)
}

