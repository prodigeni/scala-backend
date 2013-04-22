package com.wikia.phalanx

import com.twitter.util.{FuturePool, Duration, Future}
import com.twitter.conversions.time._


trait StatsGatherer {
  def apply[T](f: => T): Future[T]
  def reset(): Future[Unit]
  def cacheHit(): Future[Unit]
  def storeRequest(time: Duration, request: => ParsedRequest): Future[Unit]
  def totalTime:String
  def avgTime:String
  def statsString:String
  def longStats:String
}

object NullGatherer extends StatsGatherer {
  def apply[T](f: => T): Future[T] = Future.value(f)
  def reset(): Future[Unit] = Future.Done
  def cacheHit(): Future[Unit] = Future.Done
  def storeRequest(time: Duration, request: => ParsedRequest): Future[Unit] = Future.Done
  def totalTime:String = "unknown"
  def avgTime:String = "unknown"
  def statsString:String = ""
  def longStats:String = ""
}

class RealGatherer extends StatsGatherer {
  case class LongRequest(duration:Duration, pr: ParsedRequest) {
    override def toString:String = Seq(
      s"$duration ${pr.request.remoteHost} ${pr.request.path} ${pr.checkTypes.mkString(",")} ",
      s"[${pr.content.map(_.text.length).sum} chars] Referer: ${pr.request.headers.getOrElse("Referer","")} ",
      s"Matches: ${pr.matches.mkString(",")}"
    ).mkString
  }
  implicit object LongRequestOrdering extends Ordering[LongRequest] {
    def compare(a:LongRequest, b:LongRequest) = a.duration compare b.duration
  }


  val keepLastMinutes = Config.keepLastMinutes()
  val breakline = "\n"+("-"*80) + "\n"
  val timeRanges = Seq(0.microseconds, 100.microseconds,
    1.millis, 3.millis, 10.millis, 30.millis, 100.millis, 300.millis,
    1.seconds, 3.seconds, 10.seconds, Duration.Top).toIndexedSeq
  private val pool = FuturePool(java.util.concurrent.Executors.newSingleThreadExecutor())
  private var matchTime = 0.microsecond
  private var matchCount = 0
  private var cacheHits = 0
  private val longRequests = collection.mutable.SortedSet.empty[LongRequest]
  private var longRequestThreshold = 0.microsecond
  private val longRequestsMax = 10

  def newTimeCounts() = collection.mutable.ArrayBuffer.fill(timeRanges.length)(0L)
  var timeCounts = newTimeCounts()


  def apply[T](f: => T): Future[T] = pool.apply(f)
  def storeRequest(time: Duration, request: => ParsedRequest) = pool {
    matchTime += time
    matchCount += 1
    if (time >= longRequestThreshold || longRequests.size < longRequestsMax) {
      longRequests += LongRequest(time, request)
      if (longRequests.size > longRequestsMax) longRequests -= longRequests.head
      longRequestThreshold = longRequests.head.duration
    }
    val index = timeRanges.indexWhere( d => time < d, 1)
    if (index > 0) timeCounts(index-1) = timeCounts(index-1) + 1
  }
  def cacheHit() = pool { cacheHits += 1 }
  def reset() = pool {
    matchTime = 0.microsecond
    matchCount = 0
    cacheHits = 0
    longRequests.clear()
    longRequestThreshold = 0.microsecond
    timeCounts = newTimeCounts()
  }
  def statsString: String = (Seq(
    s"Total time spent matching: $totalTime",
    s"Average time spent matching: $avgTime",
    s"Matches done: $matchCount",
    s"User cache hits: $cacheHits",
    s"Cache hit %: ${if (matchCount+cacheHits>0) cacheHits*100/(matchCount+cacheHits) else "unknown"}",
    s"Longest request time: ${longRequests.lastOption.map(_.duration.niceString()).getOrElse("unknown")}"
  ) ++ timeBreakDown).mkString("\n")
  def timeBreakDown:Seq[String] = {
    for ( (range, count) <- timeRanges.sliding(2).toSeq.zip(timeCounts.toSeq) ) yield (
      f"Requests ${range(0).niceString()}%16s to ${range(1).niceString()}%16s: $count%10d " + (if (matchCount>0) f"(${count*100/matchCount}%2d%)" else ""))
  }
  def longStats:String = longRequests.toSeq.reverse.map(_.toString).mkString(breakline)
  def totalTime:String = matchTime.niceString()
  def avgTime: String = matchCount match {
    case 0 => "unknown"
    case x => (matchTime / x).niceString()
  }
}
