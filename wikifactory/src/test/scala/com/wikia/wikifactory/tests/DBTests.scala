package com.wikia.wikifactory.tests

import org.scalatest._
import com.twitter.util.Time

class DBTests extends FlatSpec {
  import com.wikia.wikifactory.DB

  val cal = java.util.Calendar.getInstance(DB.dbTimeZone)
  cal.clear()
  cal.set(2005, 0, 3, 5, 7, 9)  // java is brain dead: months are counted from 0
  val tDate = Time(cal.getTime)
  val wikiDate = "20050103050709"

  "Conversions" should "work with fromWikiTime" in {
    assert(DB.fromWikiTime(wikiDate) === Some(tDate))
  }

  it should "work with toWikiTime" in {
    assert( DB.toWikiTime(tDate) === wikiDate)
  }

  "infinite" should "be handled property" in {
    assert(DB.fromWikiTime("infinite") === Some(Time.Top))
    assert(DB.toWikiTime(Time.Top) == "infinite")
  }

}





