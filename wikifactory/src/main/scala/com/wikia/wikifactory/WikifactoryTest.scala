package com.wikia.wikifactory

object WikifactoryTest extends App {
  import scala.slick.driver.MySQLDriver.simple._
  object Phalanx extends Table[(Int, String)]("phalanx") {
    def id = column[Int]("p_id", O.PrimaryKey) // This is the primary key column
    def text = column[String]("p_text")
    def * = id ~ text
  }

  def test() = { /* this should not be a unittest, because it connects to real database */
    val db = new DB(DB.DB_MASTER, dbName = "wikicities").connect()
      db.withSession( { implicit session:Session =>
        val rows = Phalanx.sortBy(_.id).take(1).map(x => x.id).to[Seq]
        assert (rows.length == 1)
        assert (rows(0) == 1)
      })
    db
  }
  assert(test().isInstanceOf[Database])
  println("wikifactory test ok")
}
