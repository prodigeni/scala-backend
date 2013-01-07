import com.wikia.wikifactory._;

/**
 *  main object for testing scala classes & objects
 */
object wikifactoryTest extends App {
  val db = new DB( DB.DB_MASTER, "", "wikicities" ).connect
  val resultQuery = db.resultQuery("select city_last_timestamp from city_list limit 1")

  println( resultQuery.fetch("city_last_timestamp").toString )

  val sc = new ScribeHandler("localhost", 9090)
  val res = sc.send( "log_phalanx", Map(
    "blockId" -> 1,
    "blockType" -> 1,
    "blockTs" -> "2013-01-04 17:32:00",
    "blockUser" -> "Test",
    "city_id" -> 177
  ) )

  println ("sc res: " + res.toString)
}