package com.wikia.wikifactory

import scala.slick.driver.MySQLDriver.simple._

import org.scalatest._
import slick.session.Session

object Phalanx extends Table[(Int, String)]("phalanx") {
  def id = column[Int]("p_id", O.PrimaryKey) // This is the primary key column
  def text = column[String]("p_text")
  def * = id ~ text
}

class DBTests extends FlatSpec {
  val cal = java.util.Calendar.getInstance(DB.dbTimeZone)
  cal.clear()
  cal.set(2005, 0, 3, 5, 7, 9)  // java is brain dead: months are counted from 0
  val tDate = cal.getTime
  val wikiDate = "20050103050709"

  "Conversions" should "work with fromWikiTime" in {
    assert(DB.fromWikiTime(wikiDate) === Some(tDate))
  }

  it should "work with toWikiTime" in {
    assert( DB.toWikiTime(tDate) === wikiDate)
  }

  "Simple query" should "work" in {
    val db = new DB(DB.DB_MASTER, dbName = "wikicities").connect()
    db.withSession( { implicit session:Session =>
      val rows = Phalanx.sortBy(_.id).take(1).map(x => x.id).to[Seq]
      assert (rows.length === 1)
      assert (rows(0) === 1)
    })
  }
}





