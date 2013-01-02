import com.wikia.wikifactory._;

/**
 *  main object for testing scala classes & objects
 */
object wikifactoryTest extends App {
  val db = new DB( DB.DB_MASTER, "", "wikicities" ).connect
  println(db)
    //.connection

  db.withTransaction {
    //import scala.slick.jdbc.{StaticQuery => Q, GetResult}
    //import scala.slick.session.Database.threadLocalSession
    //var cityId = sql"select city_id from city_list order by 1 limit 11".as[String]
    //println("cityId = " + cityId.list())
  }
}