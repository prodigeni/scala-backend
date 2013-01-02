/**
 * map for $wfLBFactoryConf
 *
 * @author Krzysztof Krzyżaniak (eloy) <eloy@wikia-inc.com>
 * @author Maciej Szumocki (szumo) <szumo@wikia-inc.com>
 * @author Piotr Molski (moli) <moli@wikia-inc.com>
 */

package com.wikia.wikifactory

import scala.util.Properties._
import scala.io.Source.{fromFile}
import java.util.{ArrayList, LinkedHashMap}
import scala.collection.JavaConverters._
import org.yaml.snakeyaml._
import scala.util.{Random}
import java.util

class LBFactoryConf( sourcePath: String ) {
  type HashMapStr[T] = LinkedHashMap[ String,T ]
  type HashMapStrHash[T] = LinkedHashMap[ String, LinkedHashMap[String, T] ]
  type HashMapStrHashHash[T] = LinkedHashMap[ String, LinkedHashMap[String, LinkedHashMap[String, T]] ]

  val yamlData = (new Yaml).load( fromFile( sourcePath ).mkString ).asInstanceOf[ ArrayList[_] ]
  val section = yamlData.get(0).asInstanceOf[HashMapStr[AnyRef]]

  /* default contructor */
  def this() = this( envOrElse("WIKIA_DB_YML", "/usr/wikia/conf/current/DB.yml" ) )

  /* class in YAML config */
  def className = section.get("class").toString()

  /* sectionsByDB in YAML config */
  def sectionsDB( name: String ): String = {
    val sections = section.get("sectionsByDB").asInstanceOf[HashMapStr[String]].get( name )
    if ( sections != null && !sections.isEmpty ) sections.toString else ""
  }

  /* sectionLoads in YAML config */
  def sectionLoads( name: String ): HashMapStr[Int] =
    section.get("sectionLoads").asInstanceOf[HashMapStrHash[Int]].get(name)
  def masterFromSection( name: String ): String = {
    println("master = " + name)
    sectionLoads( name ).asScala.head._1
  }
  def slaveFromSection( name: String ): String = {
    var slave = Random.shuffle(sectionLoads( name ).asScala.keys.drop(0).toList).head
    if ( slave.isEmpty ) slave = masterFromSection( name )
    slave
  }

  /* groupLoadsBySection in YAML config */
  def groupLoadsBySection( name: String ): HashMapStrHash[Int] =
    section.get("groupLoadsBySection").asInstanceOf[HashMapStrHashHash[Int]].get(name)

  /* serverTemplate in YAML config */
  def serverTemplate =
    section.get("serverTemplate").asInstanceOf[HashMapStr[String]]

  /* hostByName in Yaml config */
  def hostsByName( name: String ) : String =
    section.get("hostsByName").asInstanceOf[HashMapStr[String]].get(name).toString()

  /* externalLoad in Yaml config */
  def externalLoads( name: String ) : HashMapStr[Int] =
    section.get("externalLoads").asInstanceOf[HashMapStrHash[Int]].get(name)

  /* templateOverridesByCluster in Yaml config */
  def templateByCluster( name: String) : HashMapStr[String] = {
    val t = section.get("templateOverridesByCluster")
    if ( t != null )
      t.asInstanceOf[HashMapStrHash[String]].get(name)
    else
      null
  }

  /* templateOverridesByServer in Yaml config */
  def templateByServer( name: String) : HashMapStr[String] = {
    val t = section.get("templateOverridesByServer")
    if ( t != null )
      t.asInstanceOf[HashMapStrHash[String]].get(name)
    else
      null
  }

  /* second main section in YAML config */
  def database( name: String ) : ArrayList[String] =
    yamlData.get(1).asInstanceOf[HashMapStr[ArrayList[String]]].get(name)

  /* third main section in YAML config */
  def external( name: String ) : ArrayList[String] =
    yamlData.get(2).asInstanceOf[HashMapStr[ArrayList[String]]].get(name)
}