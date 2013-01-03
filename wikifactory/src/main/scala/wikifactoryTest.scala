import com.wikia.wikifactory._;

/**
 *  main object for testing scala classes & objects
 */
object wikifactoryTest extends App {
  val db = new DB( DB.DB_MASTER, "", "wikicities" ).connect
  val resultQuery = db.resultQuery("select city_last_timestamp from city_list limit 1")

  println( resultQuery.fetch("city_last_timestamp").toString )
}