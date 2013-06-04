package com.wikia.phalanx

import collection.JavaConversions._
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Http, Response, Request}
import com.twitter.util.{Time, TimerTask, SynchronizedLruMap, Future, FuturePool}
import java.util.NoSuchElementException
import scala.collection.mutable

trait ParsedRequest {
  def request: Request
  def response: Response
  def content:Iterable[Checkable]
  def checkTypes:Iterable[String]
  def matches: Option[DatabaseRuleInfo]
}


class MainService(val reloader: (Map[String, RuleSystem], Traversable[Int]) => Map[String, RuleSystem],
                  val scribe: Service[Map[String, Any], Unit], notifyNodes: Seq[String] = Seq.empty  ) extends Service[Request, Response] {
  def this(initialRules: Map[String, RuleSystem], scribe: Service[Map[String, Any], Unit]) = this((_, _) => initialRules, scribe)
  val logger = NiceLogger("MainService")
  var nextExpireDate: Option[Time] = None
  var expireWatchTask: Option[TimerTask] = None
  val userCacheMaxSize = Config.userCacheMaxSize()
  val stats:StatsGatherer = if (Config.detailedStats()) new RealGatherer() else NullGatherer
  val userCache:mutable.Map[String, Option[DatabaseRuleInfo]] = new SynchronizedLruMap[String, Option[DatabaseRuleInfo]](userCacheMaxSize)
  val differences:mutable.Set[Int] = new mutable.HashSet[Int] with mutable.SynchronizedSet[Int] // for unexpected differences between old and new phalanx
  val notifyMap = {
    val localhost = java.net.InetAddress.getLocalHost.getHostName
    logger.debug(s"Local hostname: $localhost")
    logger.debug(s"Notify nodes: $notifyNodes")
    notifyNodes.filterNot(_.startsWith(localhost)).map( node => {
      val parts = node.split(":")
      val hostname = parts.head
      val port = parts.tail.headOption.map(_.toInt).getOrElse(4666)
      val client = ClientBuilder()
        .codec(Http())
        .hosts(new java.net.InetSocketAddress(hostname, port))
        .hostConnectionLimit(1)
        .daemon(true)
        .build()
      logger.info(s"Created client for cluster node notifications: $node")
      (node, client)
    }).toMap
  }
  val timer = com.twitter.finagle.util.DefaultTimer.twitter
  @transient var rules:Map[String, RuleSystem] = reloader(Map.empty, Seq.empty)
  val threadPoolSize:Int = Config.serviceThreadCount() match {
    case 0 => Seq(1, Main.processors).max
    case x => x
  }
  val futurePool = if (threadPoolSize <= 0) {FuturePool.immediatePool} else {
    FuturePool(java.util.concurrent.Executors.newFixedThreadPool(threadPoolSize, new NamedPoolThreadFactory("MainService pool")))
  }
  val reloadPool = FuturePool(java.util.concurrent.Executors.newSingleThreadExecutor())
  watchExpired()
  val favicon = Respond.fromResource("/shield.ico", "image/x-icon")
  abstract class BaseRequest extends ParsedRequest {
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
    val expected = params.get("expected").map(_.toInt)
    val combinations: Iterable[(RuleSystem, Checkable)] = (for (r <- ruleSystems;c <- content) yield (r, c))
    val cacheable:Option[String] = checkTypes match {
      case "user" :: Nil => Some(params.getAll("content").mkString("|"))
      case _ => None
    }
    lazy val matches: Option[DatabaseRuleInfo] = {
      cacheable match {
        case Some(value) if (userCache.contains(value)) => {
          stats.cacheHit()
          userCache(value)
        }
        case _ => {
          val elapsed = com.twitter.util.Stopwatch.start()
          val matches = combinations.view.flatMap((pair: (RuleSystem, Checkable)) => pair._1.firstMatch(pair._2))
          val result = matches.take(1).headOption
          result.map(sendToScribe(_))
          cacheable match {
            case Some(value) => userCache(cacheable.get) = result
            case _ => ()
          }
          stats.storeRequest(elapsed(), this)
          expected match {
            case Some(expectedId) => {
              val got = result.map(r => r.dbId).getOrElse(0)
              if (got != expectedId) {
                // old phalanx result was different, log the difference
                val first = content.head.text
                logger.warn(s"Results different in old phalanx: expected = $expectedId, got = $got, content = ${if (first.length < 60) first else "[too long]"}, refererer = ${request.referer.getOrElse("")}")
                differences ++= Seq(got, expectedId).filter(_ != 0)
              }
            }
            case _ => ()
          }
          result
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
  case class CheckRequest(request: Request) extends BaseRequest {
    def response = {
      if (ruleSystems.isEmpty) Respond.unknownType else {
        if (content.isEmpty) 	Respond.contentMissing	else {
          logger.trace(s"check: lang=$lang user=$user wiki=$wiki checkTypes=$checkTypes content=$content matches=$matches expected=$expected")
          if (matches.isEmpty) Respond.ok else Respond.failure
        }
      }
    }
  }
  case class MatchRequest(request: Request) extends BaseRequest {
    def response = {
      if (ruleSystems.isEmpty) Respond.unknownType else {
        if (content.isEmpty) Respond.contentMissing	else {
          logger.trace(s"match: lang=$lang user=$user wiki=$wiki checkTypes=$checkTypes content=$content matches=$matches expected=$expected")
          Respond.json(matches)
        }
      }
    }
  }

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
    userCache.clear()
  }
  def expiredRules = {
    val now = Time.now
    rules.values.flatMap(ruleSystem => ruleSystem.expiring.takeWhile(rule => rule.expires.get <= now)).map(r => r.dbId)
  }
  def refreshRules(expired: Traversable[Int]) {
    rules = reloader(rules, expired).toMap
    if (expired.isEmpty) {
      stats.reset()
      differences.clear()
    } else {
      differences --= expired
    }
    userCache.clear()
    watchExpired()
  }
  type checkOrMatch = (Iterable[RuleSystem], Iterable[Checkable], Option[String], Option[Int]) => Response
  def validateRegex(request: Request) = {
    val s = request.params.getOrElse("regex", "")
    InvalidRegex.checkForError(s) match {
      case None => Respond.ok
      case Some(error) => Respond.error(s"failure\n$error")
    }
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
  def statsString: String = {
    val checkerStats = rules.toSeq.map(t => {
      val (s, ruleSystem) = t
      s + ":\n" + ruleSystem.stats.map("  "+_).mkString("\n")
    }).mkString("\n")
    val response =  Seq(Main.versionString,
      s"Next rule expire date:${nextExpireDate.getOrElse("not set")}",
      s"NewRelic environment: ${sys.props.getOrElse("newrelic.environment", "not set")} (${if (Config.newRelic() && sys.props.get("newrelic.environment").nonEmpty) "enabled" else "disabled"})",
      s"Processor count: ${Main.processors}",
      s"Max heap memory: ${sys.runtime.maxMemory().humanReadableByteCount}",
      s"Total heap memory: ${sys.runtime.totalMemory().humanReadableByteCount}",
      s"Free heap memory: ${sys.runtime.freeMemory().humanReadableByteCount}",
      s"User cache: ${userCache.size} (entries) / $userCacheMaxSize (size)",
      "",
      stats.statsString,
      checkerStats
    ).mkString("\n")
    response
  }
  def checkerDescriptions: String = {
    rules.toSeq.filter(t => t._1 == "user" || t._1 == "content").map(t => {
      val (s, ruleSystem) = t
      s + ":\n" + ruleSystem.checkerDescriptions.map("  "+_).mkString("\n")
    }).mkString("\n")
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
  def findRuleById(id: Int): Option[DatabaseRuleInfo] = {
    rules.values.view.flatMap(x => x.rules.find(r => r.dbId == id)).headOption
  }
  def differencesResponse: Response = {
    val ids = differences.toIndexedSeq.sorted
    val ruleSeq = ids.flatMap(id => findRuleById(id))
    val diffPage = new DifferencesPage(ruleSeq)
    diffPage.response
  }
  def apply(request: Request): Future[Response] = {
    stripPath(request) match {
      case "" => Future(Respond("PHALANX ALIVE"))
      case "match" => futurePool { MatchRequest(request).response }
      case "check" => futurePool { CheckRequest(request).response }
      case "validate" => futurePool { validateRegex(request) }
      case "reload" => reloadPool(reload(request, true))
      case "notify" => reloadPool(reload(request, false))
      case "stats/total" => stats(Respond(stats.totalTime))
      case "stats/avg" => stats(Respond(stats.avgTime))
      case "stats/long" => stats(Respond(stats.longStats))
      case "stats/checkers" => stats(Respond(checkerDescriptions))
      case "stats.json" => stats(Respond.json(stats))
      case "differences" => futurePool { differencesResponse }
      case "stats" => stats(Respond(statsString))
      case "view" => futurePool(viewRule(request))
      case "favicon.ico" => Future.value(favicon)
      case x => {
        logger.warn("Unknown request path: " + request.path + " [ " + x.toString + " ] ")
        Future(Respond.notFound)
      }
    }
  }


}
