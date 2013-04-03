/**
 * database handler
 *
 * @author Krzysztof Krzyżaniak (eloy) <eloy@wikia-inc.com>
 * @author Maciej Szumocki (szumo) <szumo@wikia-inc.com>
 * @author Piotr Molski (moli) <moli@wikia-inc.com>
 */

package com.wikia.wikifactory

import com.twitter.util.{Time, TimeFormat}

object DB {
  val DB_MASTER       : Int = -2
  val DB_SLAVE        : Int = -1

  val EXTERNALSHARED  : String = "wikicities"
  val STATS           : String = "stats"
  val METRICS         : String = "metrics"
  val DATAWARESHARED  : String = "dataware"

  val DB_SPECIALS     = List("smw+")

  val dbTimeZone = java.util.TimeZone.getTimeZone("UTC")
  val mediaWikiTimeFormat = new TimeFormat("yyyyMMddHHmmss")

  def wikiCurrentTime = toWikiTime(Time.now)
  def toWikiTime(date: Time):String = date match {
    case Time.Top => "infinite"
    case _ => mediaWikiTimeFormat.format(date)
  }

  def fromWikiTime(dateBytes: Array[Byte]):Option[Time] = if (dateBytes == null) None else fromWikiTime(new String(dateBytes, "ascii"))
  def fromWikiTime(date: String):Option[Time] = date match {
    case "infinite" => Some(Time.Top)
    case _ => try {
      Some(mediaWikiTimeFormat.parse(date))
    } catch {
      case e:java.text.ParseException => None
    }
  }

}

case class dbException( msg:String ) extends Exception

class DB( val dbType: Int = DB.DB_SLAVE, val dbGroup: Option[String] = None, val dbName: String = DB.EXTERNALSHARED,
          val config: LBFactoryConf = new LBFactoryConf() ) {
  /* read DB.yml config */

  /* special section */
  private val special = if ( DB.DB_SPECIALS.contains( dbName ) ) dbName else ""

  /* default parameters to db connect */
  private val dbTemplate = config.serverTemplate

  try {
    /* section = special or check if dbName is defined in section */
    var section = (if (!special.isEmpty) config.sectionsDB(special) else config.sectionsDB(dbName)).toString

    if (section.isEmpty) {
      /*
        read which cluster is used for database,
        cluster for DB.EXTERNALSHARED is always known, it prevent loops
       */
      if (dbName != DB.EXTERNALSHARED) {
        val cluster = dbCluster()
        if (!cluster.isEmpty) {
          section = (if (!config.sectionsDB(cluster).isEmpty) config.sectionsDB(cluster) else cluster).toString
        }
      }
    }

    /* get db */
    val connect = if (dbType == DB.DB_MASTER) config.masterFromSection(section) else config.slaveFromSection(section)
    if (connect.isEmpty) throw new dbException("DB connection error: cannot find section `" + section + "` in config file")

    /* get special db connection */
    val templateCluster = config.templateByServer( connect )

    /* set connection parameters */
    dbTemplate.put( "dbname", dbName )
    dbTemplate.put( "host", config.hostsByName(connect) )
    if ( templateCluster != null ) dbTemplate.putAll( templateCluster )
    //println("We have: type=`" + dbType + "`, group=`" + dbGroup + "`, name=`" + dbName + "`, connect=`" + connect + "` ")
  }
  catch {
    case e: Exception => println("exception caught: " + e.getMessage + "\n\nStack:\n" + e.getStackTraceString)
  }

  def connect(): scala.slick.session.Database = {
    val dbDriver = "com." + dbTemplate.get("type") + ".jdbc.Driver"
    val dbConn = "jdbc:" + dbTemplate.get("type") + "://" + dbTemplate.get("host") + "/" + dbName

    val props = new java.util.Properties()
    props.setProperty( "zeroDateTimeBehavior", "convertToNull" )
    props.setProperty( "user", dbTemplate.get("user") )
    props.setProperty( "password", dbTemplate.get("password") )
    props.setProperty( "autoReconnect", "true")
    props.setProperty( "characterEncoding", "UTF-8" )
    props.setProperty( "failOverReadOnly", "false" )
    props.setProperty( "testOnBorrow", "true" )
    props.setProperty( "validationQuery", "SELECT 1")

    scala.slick.session.Database.forURL(dbConn, prop = props, driver = dbDriver)
  }
  // TO DO
  def lightMode() : Boolean = false
  // TO DO
  def dbCluster(): String = "central"
}