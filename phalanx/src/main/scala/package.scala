package com.wikia

import collection.mutable
import scala.language.implicitConversions
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit

package object phalanx {
	def tryNTimes[T](n: Int, block: => T): Either[Throwable, T] = {
		try {
			Right(block)
		} catch {
			case e: Throwable => if (n <= 0) Left(e) else tryNTimes(n - 1, block)
		}
	}
	implicit class RichIterableGroup[A](val obj: Iterable[A]) extends AnyVal {
		// adapted from TraversableLike.groupBy
		def groupMap[K, R](f: A => (K, R)): Map[K, Iterable[R]] = {
			val m = mutable.Map.empty[K, mutable.ListBuffer[R]]
			for (elem <- obj) {
				val (key, value) = f(elem)
				val seq: mutable.ListBuffer[R] = m.getOrElseUpdate(key, {mutable.ListBuffer.empty[R]})
				seq += value
			}
			m.mapValues(buf => buf.toList).toMap
		}
		def groupCount[K](f: A => K): Map[K, Int] = {
			val counter = mutable.Map.empty[K, Int].withDefaultValue(0)
			for (elem <- obj) {
				val key = f(elem)
				counter(key) = counter(key) + 1
			}
			counter.toMap
		}
	}
	implicit class HumanReadableByteCount(val bytes: Long) extends AnyVal {
		def humanReadableByteCount: String = {
			val unit = 1024
			if (bytes < unit) {
				bytes + " B"
			}
			else {
				val exp = (scala.math.log(bytes) / scala.math.log(unit)).toInt
				val pre = "KMGTPE".charAt(exp - 1) + "iB"
				f"${bytes / scala.math.pow(unit, exp)}%.1f $pre"
			}
		}
	}
  val timeUnits = Seq(
    TimeUnit.DAYS,
    TimeUnit.HOURS,
    TimeUnit.MINUTES,
    TimeUnit.SECONDS,
    TimeUnit.MILLISECONDS,
    TimeUnit.MICROSECONDS,
    TimeUnit.NANOSECONDS)
  implicit class NiceDuration(val d:Duration) {
    def niceString(parts:Int = 2):String = {
      d match {
        case Duration.Bottom => "0"
        case Duration.Top => "infinity"
        case _ if d.inNanoseconds == 0 => "0"
        case _ => {
          var left = parts
          val s = new StringBuilder
          var ns = d.inNanoseconds
          for (u <- timeUnits) {
            val v = u.convert(ns, TimeUnit.NANOSECONDS)
            if (v != 0 && left > 0) {
              ns -= TimeUnit.NANOSECONDS.convert(v, u)
              if (v > 0 && !s.isEmpty)
                s.append(", ")
              s.append(v.toString)
              s.append(" ")
              s.append(u.name.toLowerCase)
              left = left - 1
            }
          }
          s.toString()
        }
      }
    }

  }

}


