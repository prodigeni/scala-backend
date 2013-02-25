package com.wikia.phalanx

import scala.slick.driver.MySQLDriver.simple._

object RuleSystemLoader {
	case class PhalanxRecord(id: Int, pcase: Int, exact: Int, regex: Int, author: Int, ptype: Int, textBlob: Array[Byte],
	                         reasonBlob: Array[Byte], expire: Option[String], lang: Option[String]) {
		def text = new String(textBlob, "UTF-8")
		def reason = new String(reasonBlob, "UTF-8")
	}

	object PhalanxTable extends Table[PhalanxRecord]("phalanx") {
		def id = column[Int]("p_id", O.PrimaryKey)
		def pcase = column[Int]("p_case")
		def exact = column[Int]("p_exact")
		def regex = column[Int]("p_regex")
		def author = column[Int]("p_author_id")
		def ptype = column[Int]("p_type")
		def text = column[Array[Byte]]("p_text")
		def reason = column[Array[Byte]]("p_reason")
		def expire = column[Option[String]]("p_expire")
		def lang = column[Option[String]]("p_lang")
		// Every table needs a * projection with the same type as the table's type parameter
		def * = id ~ pcase ~ exact ~ regex ~ author ~ ptype ~ text ~ reason ~ expire ~ lang <>(PhalanxRecord, PhalanxRecord.unapply _)
	}

	val logger = NiceLogger("RuleSystemLoader")

	val contentTypes = Map(// bitmask to types
		(1, "content"), // const TYPE_CONTENT = 1;
		(2, "summary"), // const TYPE_SUMMARY = 2;
		(4, "title"), // const TYPE_TITLE = 4;
		(8, "user"), // const TYPE_USER = 8;
		(16, "question_title"), // const TYPE_ANSWERS_QUESTION_TITLE = 16;
		(32, "recent_questions"), // const TYPE_ANSWERS_RECENT_QUESTIONS = 32;
		(64, "wiki_creation"), // const TYPE_WIKI_CREATION = 64;
		(128, "cookie"), // const TYPE_COOKIE = 128;
		(256, "email") // const TYPE_EMAIL = 256;
	)

	def makeDbInfo(row: PhalanxRecord) = {
		val lang = row.lang
		val date = row.expire

		new DatabaseRule(row.text, row.id, row.reason,
			row.pcase == 1, row.exact == 1, row.regex == 1, lang,
			date match {
				case None => None
				case Some(x) => com.wikia.wikifactory.DB.fromWikiTime(x)
			},
			row.author, row.ptype)
	}
	private def ruleBuckets = (for (v <- contentTypes.values) yield (v, collection.mutable.Set.empty[DatabaseRule])).toMap
	private def dbRows(db: Database, ids: Option[Set[Int]]): Seq[PhalanxRecord] = {
		logger.info("Getting database rows")
		val rows = tryNTimes(3, {
			db.withSession({
				implicit session: Session =>
					db.withTransaction {
						val current = com.wikia.wikifactory.DB.wikiCurrentTime
						val query = Query(PhalanxTable).filter(x => x.expire.isNull || x.expire.>(current))
						val query2 = ids match {
							case None => query
							case Some(s) => query.filter(x => x.id.inSet(s))
						}
						logger.debug(s"Issuing query with session=$session")
						query2.list
					}
			})
		}) match {
			case Left(e: Throwable) => throw e
			case Right(x) => x.toIndexedSeq
		}
		logger.info("Got " + rows.length + " rows")
		rows
	}
	private def createRules(rows: Seq[PhalanxRecord]) = {
		val result = ruleBuckets
		val ids = collection.mutable.Set.empty[Int]
		for (row: PhalanxRecord <- rows) {
			try {
				val rule = makeDbInfo(row)
				val t = row.ptype
				for ((i: Int, s: String) <- contentTypes) {
					if ((i & t) != 0) result(s) += rule
				}
				ids += rule.dbId
			}
			catch {
				case e: java.util.regex.PatternSyntaxException => None
				case e: Throwable => throw e
			}
		}
		(result, ids)
	}
	def fromDatabase(db: Database): Map[String, RuleSystem] = {
		val rows = dbRows(db, None)
		val (result, _) = createRules(rows)
		result.transform((_, rules) => new CombinedRuleSystem(rules))
	}
	def reloadSome(db: Database, oldMap: Map[String, RuleSystem], changedIds: Set[Int]): Map[String, RuleSystem] = {
		if (changedIds.isEmpty) {
			// no info, let's do a full reload
			fromDatabase(db)
		} else {
			val rows = dbRows(db, Some(changedIds))
			logger.debug(s"Rows1 for reload: \n $rows")
			val (result, foundIds) = createRules(rows)
			val deletedIds = changedIds.diff(foundIds)
			logger.debug(s"reloadSome: new/changed rules: $result")
			logger.debug(s"reloadSome: deletedRules: $deletedIds")
			oldMap.transform((key, rs) => rs.reloadRules(result(key), deletedIds))
		}
	}
}


