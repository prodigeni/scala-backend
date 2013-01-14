import collection.immutable.HashMap
import com.wikia.wikifactory._;

/**
 *  main object for testing scala classes & objects
 */
object wikifactoryTest extends App {
	val db = new DB( DB.DB_MASTER, "", "wikicities" ).connect
	val resultQuery = db.resultQuery("select city_last_timestamp from city_list limit 1")

	println( "test: " + resultQuery.fetch("city_last_timestamp").toString )

  var sc = new ScribeLogger(9090).category("log_phalanx" )
    .send( HashMap(
      "blockId" -> 1,
      "blockType" -> 1,
      "blockTs" -> "2013-01-04 17:32:00",
      "blockUser" -> "Test",
      "city_id" -> 177
    ) )

  println( "sc = " + sc )

}