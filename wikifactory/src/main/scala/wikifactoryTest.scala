import collection.immutable.HashMap
import com.wikia.wikifactory._;

/**
 *  main object for testing scala classes & objects
 */
object wikifactoryTest extends App {
  var i = 0;
  while ( i < 2 ) {
    val db = new DB( DB.DB_MASTER, "", "wikicities" ).connect
    if ( db != null ) {
      val resultQuery = db.resultQuery("select city_last_timestamp from city_list limit 1")
      println( "test: " + resultQuery.fetch("city_last_timestamp").toString )
      i += 1
      println( "sleep" )
      Thread.sleep(200000)
    } else {
      println( "No connection" );
    }
  }

  /*var sc = new ScribeLogger().category("log_phalanx" )
    .sendMaps( Seq(Map(
      "blockId" -> 1,
      "blockType" -> 1,
      "blockTs" -> "2013-01-04 17:32:00",
      "blockUser" -> "Test",
      "city_id" -> 177
    )) )

  println( "sc = " + sc ) */
}