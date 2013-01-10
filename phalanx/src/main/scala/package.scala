package com.wikia

import collection.{mutable, immutable}

package object phalanx {
  implicit def string2Checkable(s:String): Checkable = new Checkable(s)
	implicit def traversable2Group[A, T <: Traversable[A]](obj: T):RichTraversableGroup[A] = new RichTraversableGroup(obj)
}

class RichTraversableGroup[A](val obj: Traversable[A]) {
	// adapted from TraversableLike.groupBy
	def groupMap[K,R](f: A => (K,R)): Map[K, Traversable[R]] = {
		val m = mutable.Map.empty[K, mutable.ListBuffer[R]]
		for (elem <- obj) {
			val (key, value) = f(elem)
			val seq:mutable.ListBuffer[R] = m.getOrElseUpdate(key, { mutable.ListBuffer.empty[R] } )
			seq += value
		}
		m.mapValues( buf => buf.toList).toMap
	}
	def groupCount[K](f: A => K): Map[K, Int] = {
		val counter = mutable.Map.empty[K, Int].withDefaultValue(0)
		for (elem <- obj) {
			val key = f(elem)
			counter.update(key, counter(key) + 1)
		}
		counter.toMap
	}
}