package com.wikia


package object phalanx {
  implicit def string2Checkable(s:String): Checkable = new Checkable(s)
}