/**
 * database handler
 *
 * @author Krzysztof Krzyżaniak (eloy) <eloy@wikia-inc.com>
 * @author Maciej Szumocki (szumo) <szumo@wikia-inc.com>
 * @author Piotr Molski (moli) <moli@wikia-inc.com>
 */

package com.wikia.wikifactory

import collection.JavaConversions._
import java.sql.DriverManager
import org.jooq._
import org.jooq.impl._
import org.jooq.impl.Factory._

object DB {
  val DB_MASTER       : Int = -2
  val DB_SLAVE        : Int = -1

  val EXTERNALSHARED  : String = "wikicities"
  val STATS           : String = "stats"
  val METRICS         : String = "metrics"
  val DATAWARESHARED  : String = "dataware"

  val DB_SPECIALS     = List("smw+")

  val mediaWikiTimeFormat:java.text.DateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
  def wikiCurrentTime =  wikiTime(new java.util.Date())
  def wikiTime(date: java.util.Date) = mediaWikiTimeFormat.format(date)
}

case class dbException( msg:String ) extends Exception

class DB( var dbType: Int = DB.DB_SLAVE, var dbGroup: String = None.toString(), var dbName: String = DB.EXTERNALSHARED ) {
  /* mysql driver */
  private var driverLoaded = false

  /* read DB.yml config */
  private val config = new LBFactoryConf()

  /* special section */
  private var special = if ( DB.DB_SPECIALS.contains( dbName ) ) dbName else ""

  /* default parameters to db connect */
  private var dbTemplate = config.serverTemplate

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

  def connect() : Factory = {
    val dbDriver = "com." + dbTemplate.get("type") + ".jdbc.Driver"
    val dbConn = "jdbc:" + dbTemplate.get("type") + "://" + dbTemplate.get("host") + "/" + dbName +
      "?zeroDateTimeBehavior=convertToNull"
    Class.forName( dbDriver )

    val dialect = dbTemplate.get("type") match {
      case "mysql" => SQLDialect.MYSQL
      case "postgresql" => SQLDialect.POSTGRES
    }

    val c = DriverManager.getConnection( dbConn, dbTemplate.get("user"), dbTemplate.get("password") )
    new Factory(c, dialect)
  }
  // TO DO
  def lightMode() : Boolean = false
  // TO DO
  def dbCluster(): String = "central"
}