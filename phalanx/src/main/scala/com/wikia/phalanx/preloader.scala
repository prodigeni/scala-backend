package com.wikia.phalanx

import java.util.zip.ZipFile
import collection.JavaConverters._
import util.matching.Regex

/**
 * This class can be used to preload all the classes in the same package.
 *
 * Converted from Java code by Aleksander Adamowski <a href="mailto:aleksander.adamowski@gmail.com">aleksander.adamowski@gmail.com</a>
 * that was at http://olo.org.pl/dr/files/PackagePreloader.java.txt , licesed with BSD license.
 *
 * Might be useful to move this, NiceLogger and perhaps some other utils into wikifactory lib.
 *
 */
object PackagePreloader {
	/**
	 * Preload all the classes in the one package.
	 * Returns classes loaded.
	 */
	def apply(mainClass: Class[_], prefixes: Iterable[String]): Iterable[Class[_]] = {
		val source = mainClass.getProtectionDomain.getCodeSource
		if (source == null) throw new Exception("Cannot get the code location for class " + mainClass.toString)
		val location = source.getLocation
		if (location.getProtocol == "file" && location.toString.endsWith(".jar")) {
			val pattern: Regex = ("^("+prefixes.map(java.util.regex.Pattern.quote).mkString("|")+").*\\.class$").r
			val path = location.toString.stripPrefix("file:")
			val f = new java.io.File(path)
      try {
			  val jarZipFile = new ZipFile(f)

			val entries = jarZipFile.entries.asScala.toStream
			val filtered = entries.map(s => s.toString).map(s=>s.replace('/', '.')).flatMap(pattern.findFirstIn _)
			val result = filtered.map(x => Class.forName(x.stripSuffix(".class")))
			result.force.toSet
      } catch {
        case e: java.util.zip.ZipException => Set.empty
        case e: java.io.FileNotFoundException => Set.empty
        case x: Throwable => throw x
      }
		}
		else Set.empty
	}
}
