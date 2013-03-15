package com.wikia.phalanx

import collection.JavaConversions._
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.http.RichHttp
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.http.{Http, Request, Status, Version, Response, Message}
import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.util.{Future, Time, TimerTask, FuturePool}
import com.wikia.wikifactory._
import java.io.{FileInputStream, File}
import java.util.NoSuchElementException
import java.util.regex.PatternSyntaxException
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import util.parsing.json.{JSONObject, JSONArray, JSONFormat}
import com.twitter.finagle.tracing.{TraceId, Record}

class ExceptionLogger[Req, Rep](val logger: NiceLogger) extends SimpleFilter[Req, Rep] {
	def this(loggerName: String) = this(NiceLogger(loggerName))
	def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
		service(request).onFailure((exception) => {
			logger.exception("Exception in service", exception)
		})
	}
}

class Tracer(val logger:NiceLogger) extends com.twitter.finagle.tracing.Tracer {
  def record(record: Record) {
    logger.trace(record.toString())
  }

  def sampleTrace(traceId: TraceId): Option[Boolean] = Some(true)
}

object Tracer {
  def apply(logger: NiceLogger) = if (logger.logger.isTraceEnabled) new Tracer(logger) else com.twitter.finagle.tracing.NullTracer
}

object Respond {
	def apply(content: String, status: org.jboss.netty.handler.codec.http.HttpResponseStatus = Status.Ok, contentType: String = "text/plain; charset=utf-8") = {
		val response = Response(Version.Http11, status)
		response.contentType = contentType
		response.contentString = content
		response
	}
	def json(data: Iterable[DatabaseRuleInfo]) = {
		val jsonData = JSONArray(data.toList.map(x => x.toJSONObject))
		Respond(jsonData.toString(JSONFormat.defaultFormatter), Status.Ok, Message.ContentTypeJson)
	}
	def json(data: Map[String, DatabaseRuleInfo]) = {
		val jsonData = JSONObject(data.mapValues(x => x.toJSONObject))
		Respond(jsonData.toString(JSONFormat.defaultFormatter), Status.Ok, Message.ContentTypeJson)
	}
	def error(info: String, status: HttpResponseStatus = Status.InternalServerError) = Respond(info + "\n", status)
	val ok = Respond("ok\n")
	val failure = Respond("failure\n")
	val contentMissing = error("content parameter is missing", Status.BadRequest)
	val unknownType = error("Unknown type parameter", Status.BadRequest)
  val internalError = error("Internal server error")
  val notFound = error("not found", Status.NotFound)
}

class MainService(val reloader: (Map[String, RuleSystem], Traversable[Int]) => Map[String, RuleSystem],
	val scribe: Service[Map[String, Any], Unit], threadCount: Option[Int] = None, notifyNodes: Seq[String] = Seq.empty  ) extends Service[Request, Response] {
	def this(initialRules: Map[String, RuleSystem], scribe: Service[Map[String, Any], Unit]) = this((_, _) => initialRules, scribe)
	private val logger = NiceLogger("MainService")
	var nextExpireDate: Option[Time] = None
	var expireWatchTask: Option[TimerTask] = None
  val notifyMap = {
    val hostname = java.net.InetAddress.getLocalHost.getHostName
    logger.debug(s"Local hostname: $hostname")
    notifyNodes.filterNot(x => x == hostname).map( node => {
      val client = ClientBuilder()
        .codec(Http())
        .hosts(new java.net.InetSocketAddress(node, 4666)) // TODO: port hardcoded for now
        .hostConnectionLimit(1)
        .daemon(true)
        .build()
      logger.info(s"Created client for cluster node notifications: $node")
      (node, client)
    }).toMap
  }
	val timer = com.twitter.finagle.util.DefaultTimer.twitter
	@transient var rules = reloader(Map.empty, Seq.empty)
  //var rules = reloader(Map.empty, Seq.empty)
	val threadPoolSize = threadCount.getOrElse(Runtime.getRuntime.availableProcessors()*2)
	val futurePool = if (threadPoolSize <= 0) {FuturePool.immediatePool} else {
		FuturePool(java.util.concurrent.Executors.newFixedThreadPool(threadPoolSize, new NamedPoolThreadFactory("MainService pool")))
	}
  val reloadPool = FuturePool(java.util.concurrent.Executors.newSingleThreadExecutor())
	watchExpired()

	override def close(deadline: Time) = {
		logger.trace("Stopping expired timer")
		timer.stop()
		logger.trace("Stopping scribe client")
		scribe.close(deadline)
	}
  def cancelExpiredTask() {
    expireWatchTask.map(task => {
      task.cancel()
      nextExpireDate = None
      expireWatchTask = None
      logger.debug("Old expire task cancelled")
    })
  }
	def watchExpired() {
		val minDates = rules.values.flatMap(ruleSystem => ruleSystem.expiring.headOption.map(rule => rule.expires.get))
		if (minDates.isEmpty) {
      cancelExpiredTask()
			logger.debug("Expire task not required")
		} else {
      val expireDate = Some(minDates.min + 1.second)
      if (nextExpireDate != expireDate) {
        cancelExpiredTask()
        nextExpireDate = expireDate
        expireWatchTask = Some(timer.schedule(nextExpireDate.get) { expire() })
        logger.debug(s"Scheduling expire task at ${nextExpireDate.get}")
      }
		}
	}
	def expire() {
		expireWatchTask = None
    nextExpireDate = None
		val expired = expiredRules
		logger.debug(s"Performing expire task - expired rule count: ${expired.size}")
		if (expired.isEmpty) watchExpired() else refreshRules(expired)
	}
	def expiredRules = {
		val now = Time.now
		rules.values.flatMap(ruleSystem => ruleSystem.expiring.takeWhile(rule => rule.expires.get <= now)).map(r => r.dbId)
	}
	def refreshRules(expired: Traversable[Int]) {
		rules = reloader(rules, expired).toMap
		watchExpired()
	}
	type checkOrMatch = (Iterable[RuleSystem], Iterable[Checkable], Option[String], Option[Int]) => Response
	def validateRegex(request: Request) = {
		val s = request.params.getOrElse("regex", "")
		val response = try {
			s.r
			Respond.ok
		} catch {
			case e: PatternSyntaxException => Respond.failure
      case x: Throwable => {
        logger.exception("Unexcepted error in validateRegex: ", x)
        Respond.internalError
      }
		}
		response
	}
  def sendNotify(ids: Seq[Int]):Future[Unit] = {
    val request: Request = if (ids.isEmpty) Request("/notify") else Request("/notify", "changed" -> ids.mkString(","))
    val responses = notifyMap.map( (pair) => {
      val (node, client) = pair
      // Issue a request, get a response:
      client(request).within(timer, 10.seconds)
      .onFailure( (exc) => logger.debug(s"Could not notify node $node: $exc"))
      .onSuccess( (x) => x.getStatus.getCode match {
        case 200 => ()
        case _ => logger.debug(s"Notify response from $node: $x")
        })
    })
    Future.join(responses.toSeq)
  }
	def reload(request: Request, notify: Boolean) = {
		val changed = request.getParams("changed").mkString(",").split(',').toSeq.filter(_ != "") // support both multiple and comma-separated params
		val ids = if (changed.isEmpty) Seq.empty[Int] else changed.map(_.toInt)
    refreshRules(ids)
    if (notify) sendNotify(ids).within(timer, 20.seconds).onFailure( (exc) => logger.warn(s"Could not notify all nodes due to $exc."))
    Respond.ok
	}
	def stats(request: Request): Response = Respond(statsString)
	def statsString: String = {
		val response = (rules.toSeq.map(t => {
			val (s, ruleSystem) = t
			s + ":\n" + (ruleSystem.stats.map {
				"  " + _
			}.mkString("\n")) + "\n"
		}) ++ nextExpireDate.map("Next rule expire date: " + _.toString)
			++ sys.props.get("newrelic.environment").map("NewRelic environment: " + _)
			++ Seq(
			Main.versionString,
			"Worker threads: " + threadPoolSize,
			"Max memory: " + sys.runtime.maxMemory().humanReadableByteCount,
			"Free memory: " + sys.runtime.freeMemory().humanReadableByteCount,
			"Total memory: " + sys.runtime.totalMemory().humanReadableByteCount,
			""
		)).mkString("\n")
		response
	}
	def viewRule(request: Request): Response = {
		val id = request.params.getInt("id").get
		val foundMap = rules.toSeq.map(p => {
			val (typeName: String, ruleSystem: RuleSystem) = p
			ruleSystem.rules.find(r => r.dbId == id) match {
				case Some(rule) => Seq((typeName, rule))
				case None => Seq.empty
			}
		}).flatten.toMap
		logger.debug(s"found rules: $foundMap")
		Respond.json(foundMap)
	}
	def stripPath(request: Request): String = {
		val requestPath = request.path
		val result = (if (requestPath.startsWith("http://")) {
			val afterPrefix = requestPath.substring("http://".length)
			afterPrefix.indexOf('/') match {
				case -1 => ""
				case x: Int => afterPrefix.substring(x + 1)
			}
		} else {
			requestPath.stripPrefix("/")
		}).stripSuffix("/") // get rid of '/' at the end too
    if (result != "") logger.debug {
      val params = request.params.iterator.map((p) => s"${p._1}=${p._2}").toSeq.sorted.mkString(" ")
      s"${request.remoteHost} $requestPath $params"
    }
    result
  }

	def apply(request: Request): Future[Response] = {
		stripPath(request) match {
			case "" => Future(Respond("PHALANX ALIVE"))
			case "match" => futurePool(ParsedRequest(request).matchResponse)
			case "check" => futurePool(ParsedRequest(request).checkResponse)
			case "validate" => futurePool(validateRegex(request))
			case "reload" => reloadPool(reload(request, true))
      case "notify" => reloadPool(reload(request, false))
			case "stats" => futurePool(stats(request))
			case "view" => futurePool(viewRule(request))
			case x => {
				logger.warn("Unknown request path: " + request.path + " [ " + x.toString + " ] ")
				Future(Respond.notFound)
			}
		}
	}

	case class ParsedRequest(request: Request) {
		val params = request.params
		val lang = params.get("lang") match {
			case None => "en"
			case Some("") => "en"
			case Some(x) => x
		}
		val content = params.getAll("content").map(s => Checkable(s, lang))
		val user = params.get("user")
		val wiki = params.get("wiki").map(_.toInt)
		val checkTypes = params.getAll("type")
		val ruleSystems: Iterable[RuleSystem] = if (checkTypes.isEmpty) rules.values else {
			try {
				checkTypes.map(s => rules(s)).toSet
			} catch {
				case _: NoSuchElementException => Set.empty
			}
		}
		val combinations: Iterable[(RuleSystem, Checkable)] = (for (r <- ruleSystems;c <- content) yield (r, c))

		def findMatches(limit: Int): Seq[DatabaseRuleInfo] = {
			val matches = combinations.view.flatMap((pair: (RuleSystem, Checkable)) => pair._1.allMatches(pair._2))
			val result: Iterable[DatabaseRuleInfo] = (if (limit > 0) matches.take(limit).force else matches.force)
			result.headOption.map(sendToScribe(_))
			result.toSeq
		}
		def checkResponse = {
			if (ruleSystems.isEmpty) Respond.unknownType else {
				if (content.isEmpty) 	Respond.contentMissing	else {
					val matches = findMatches(1)
					logger.trace(s"check: lang=$lang user=$user wiki=$wiki checkTypes=$checkTypes content=$content matches=$matches")
					if (matches.isEmpty) Respond.ok else Respond.failure
				}
			}
		}
		def matchResponse = {
			if (ruleSystems.isEmpty) Respond.unknownType else {
        if (content.isEmpty) Respond.contentMissing	else {
					val limit = request.params.getIntOrElse("limit", 1)
					val matches = findMatches(limit)
					logger.trace(s"match: lang=$lang user=$user wiki=$wiki checkTypes=$checkTypes content=$content matches=$matches")
					Respond.json(matches)
				}
			}
		}
		def sendToScribe(rule: DatabaseRuleInfo): Future[Unit] = {
			if (user.isDefined && wiki.isDefined) {
				scribe(Map(
					"blockId" → rule.dbId,
					"blockType" → rule.typeMask,
					"blockTs" → com.wikia.wikifactory.DB.wikiCurrentTime,
					"blockUser" → user.get,
					"city_id" → wiki.get
				))
			}
			else Future.Done
		}
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
	def wikiaProp(key: String) = sys.props("com.wikia.phalanx." + key)
	def scribeClient() = {
		val host = wikiaProp("scribe.host")
		val port = wikiaProp("scribe.port").toInt
		new ScribeClient("log_phalanx", host, port)
	}

	val cfName: Option[String] = sys.props.get("phalanx.config") orElse {
		// load config from first config file that exists
		Seq(
			"phalanx.properties",
			"/usr/wikia/conf/current/phalanx.properties",
			"/usr/wikia/docroot/phalanx.properties",
			"phalanx.default.properties",
			"/usr/wikia/phalanx/phalanx.default.properties")
			.find(fileName => {
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
	val logger = NiceLogger("Main")
	val versionString = s"Phalanx server version ${PhalanxVersion.version}"
	logger.info(s"$versionString starting, properties loaded from ${cfName.get}")
	val preloaded = PackagePreloader(this.getClass, Seq(
		"com.wikia.phalanx",
		"com.twitter.finagle.http",
		"com.twitter.util"
	))
	logger.info("Preloaded " + preloaded.size + " classes")
	val scribe = {
		val scribetype = wikiaProp("scribe")
		logger.info("Creating scribe client (" + scribetype + ")")
		scribetype match {
			case "send" => scribeClient()
			case "buffer" => new ScribeBuffer(scribeClient(), wikiaProp("scribe.flushperiod").toInt.milliseconds)
			case "discard" => new ScribeDiscard()
		}
	}
	val port = wikiaProp("port").toInt
	val threadCount: Option[Int] = wikiaProp("threads") match {
		case s: String if (s != null && s.nonEmpty) => Some(s.toInt)
		case _ => None
	}
  val notifyNodes = wikiaProp("notifynodes") match {
    case s: String if (s != null && s.nonEmpty) => s.split(' ').toSeq
    case _ => Seq.empty
  }
  val database = new DB(DB.DB_MASTER, None, "wikicities")
  logger.info("Connecting to database from configuration file " + database.config.sourcePath)
	val dbSession = database.connect()
  logger.info("Loading rules from database")
  val loader:RuleSystemLoader = CombinedLoader
	val mainService = new MainService(
		(old, changed) => loader.reloadSome(dbSession, old, changed.toSet),
		new ExceptionLogger(logger) andThen scribe,
		threadCount,
    notifyNodes
	)
	val config = ServerBuilder()
		.codec(RichHttp[Request](Http()))
		.name("Phalanx")
    .maxConcurrentRequests(600)
		.sendBufferSize(64*1024)
		.recvBufferSize(512*1024)
    .cancelOnHangup(false)
		.backlog(1000)
    .keepAlive(true)
    .hostConnectionMaxIdleTime(3.seconds)
		.bindTo(new java.net.InetSocketAddress(port))

	val server = config.build(ExceptionFilter andThen NewRelic andThen mainService)
	logger.info(s"Listening on port: $port")
	logger.trace("Initial stats: \n" + mainService.statsString)

	sys.addShutdownHook {
		logger.warn("Terminating")
		server.close(3.seconds)
		mainService.close()
		logger.warn("Shutdown complete")
	}
}

