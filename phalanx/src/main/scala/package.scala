package com.wikia

import org.jboss.netty.buffer
import org.jboss.netty.util.CharsetUtil._

package object phalanx {
  implicit def string2RuleDatabaseInfo(s: String) = new RuleDatabaseInfo(s, 0, "")
  implicit def string2Checkable(s:String): Checkable = new Checkable(s)
}