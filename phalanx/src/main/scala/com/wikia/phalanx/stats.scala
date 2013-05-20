package com.wikia.phalanx

import com.twitter.util.{FuturePool, Duration, Future, Time}
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
  val longRequestsMax = 10
  val breakline = "\n"+("-"*80) + "\n"
  val timeRanges = Seq(0.microseconds, 200.microseconds, 500.microseconds,
    1.millis, 2.millis, 5.millis, 10.millis, 20.millis, 50.millis, 100.millis, 200.millis, 500.millis,
    1.seconds, 2.seconds, 5.seconds, 10.seconds, Duration.Top).toIndexedSeq
  private val pool = FuturePool(java.util.concurrent.Executors.newSingleThreadExecutor())
  type TimeCounts = Array[Long]

  class SubStats(val since:Time, var matchCount: Int, var cacheHits: Int, var matchTime:Duration, var longRequestThreshold:Duration,
                 val longRequests:collection.mutable.SortedSet[LongRequest], val counts: TimeCounts ) {
    def this() = this(Time.now, 0, 0, 0.microsecond, 0.microsecond,
      collection.mutable.SortedSet.empty[LongRequest], Array.fill(timeRanges.length)(0L))
    override def toString =  (Seq(
      s"Total time spent matching: $totalTime",
      s"Average time spent matching: $avgTime",
      s"Cache hit %: ${if (matchCount+cacheHits>0) cacheHits*100/(matchCount+cacheHits) else "unknown"}",
      s"Longest request time: ${longRequests.lastOption.map(_.duration.niceString()).getOrElse("unknown")}",
      s"User cache hits: $cacheHits",
      f"Requests total: $matchCount%41d"
      ) ++ timeBreakDown).mkString("\n")
    def timeBreakDown:Seq[String] = {
      for ( (range, count) <- timeRanges.sliding(2).toSeq.zip(counts.toSeq) ) yield (
        f"Requests ${range(0).niceString()}%16s to ${range(1).niceString()}%16s: $count%10d " + (if (matchCount>0) f"(${count*100/matchCount}%2d%)" else ""))
    }
    def longStats:String = longRequests.toSeq.reverse.map(_.toString).mkString(breakline)
    def totalTime:String = matchTime.niceString()
    def avgTime: String = matchCount match {
      case 0 => "unknown"
      case x => (matchTime / x).niceString()
    }

  }
  def aggregate(subs: Iterable[SubStats]):Option[SubStats] = {
    if (subs.isEmpty) None else {
      def addCounts(a:Iterable[Long], b:Iterable[Long]):Iterable[Long] = a.zip(b).map(x => x._1 + x._2)
      val temp = subs.flatMap(s => s.longRequests)
      val longRequests = collection.mutable.SortedSet.empty[LongRequest]
      longRequests ++= temp.drop(Seq(0, temp.size - longRequestsMax).max)
      val result = new SubStats(
        subs.map(s => s.since).min,
        subs.map(s => s.matchCount).sum,
        subs.map(s => s.cacheHits).sum,
        Duration.fromNanoseconds(subs.map(s => s.matchTime.inNanoseconds).sum),
        subs.map(s => s.longRequestThreshold).min,
        longRequests,
        subs.map(s => s.counts.toIndexedSeq).reduce(addCounts).toArray)
      Some(result)
    }
  }

  var full = new SubStats()
  var last = collection.mutable.Map.empty[Int, SubStats]

  def apply[T](f: => T): Future[T] = pool.apply(f)
  def dropOldTimes(time: Int) {
    val dropOff = time - keepLastMinutes
    last --= last.keys.filter(p => p<=dropOff)
  }
  def subStatsForTime(time: Int): SubStats = {
    last.get(time) match {
      case Some(substats) => substats
      case None => {
        dropOldTimes(time)
        val substats = new SubStats()
        last(time) = substats
        substats
      }
    }
  }
  def subStatsForTime: SubStats = subStatsForTime(Time.now.inMinutes)
  def dropOldTimes() { dropOldTimes(Time.now.inMinutes) }
  def subStats = Seq(full, subStatsForTime)
  def storeRequest(time: Duration, request: => ParsedRequest) = pool {
    val index = timeRanges.indexWhere( d => time < d, 1)
    for (substats <- Seq(full, subStatsForTime)) {
      substats.matchTime += time
      substats.matchCount += 1
      if (index > 0) substats.counts(index-1) = substats.counts(index-1) + 1
      if (time >= substats.longRequestThreshold || substats.longRequests.size < longRequestsMax) {
        substats.longRequests += LongRequest(time, request)
        if (substats.longRequests.size > longRequestsMax) substats.longRequests -= substats.longRequests.head
        substats.longRequestThreshold = substats.longRequests.head.duration
      }
    }
  }
  def cacheHit() = pool { subStats.foreach(s => s.cacheHits += 1)}
  def reset() = pool {  full = new SubStats() }
  def statsString: String = {
    dropOldTimes()
    val now = Time.now.toString()
    def label(sub:SubStats, period:String):String = s"Stats $period:\n  From ${sub.since} to $now\n"+sub.toString.split('\n').map(s => "  "+s).mkString("\n")+"\n\n"
    label(full, "since last full reload") + aggregate(last.values).map(s => label(s, s"for last $keepLastMinutes minutes")).getOrElse("")
  }
  def longStats:String = full.longStats
  def totalTime:String = full.totalTime
  def avgTime: String = full.avgTime
}
