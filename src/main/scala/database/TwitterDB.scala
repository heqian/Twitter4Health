package name.heqian.Twitter4Health

import scala.slick.driver.H2Driver.simple._
import Database.threadLocalSession

class TwitterDB(val database: Database) {
	object Users extends Table[(Long, String, String, String, String, String, Boolean, Long, Long, Long, Long, Long, Long, Long, String, Boolean, Boolean, String)]("USERS") {
		def id = column[Long]("ID", O.PrimaryKey)
		def name = column[String]("NAME", O.Nullable)
		def screenName = column[String]("SCREEN_NAME", O.Nullable)
		def location = column[String]("LOCATION", O.Nullable)
		def url = column[String]("URL", O.Nullable)
		def description = column[String]("DESCRIPTION", O.Nullable)
		def isProtected = column[Boolean]("IS_PROTECTED")
		def followersCount = column[Long]("FOLLOWERS_COUNT", O.Nullable)
		def friendsCount = column[Long]("FRIENDS_COUNT", O.Nullable)
		def listedCount = column[Long]("LISTED_COUNT", O.Nullable)
		def favouritesCount = column[Long]("FAVOURITES_COUNT", O.Nullable)
		def statusesCount = column[Long]("STATUSES_COUNT", O.Nullable)
		def createdAt = column[Long]("CREATED_AT", O.Nullable)
		def utcOffset = column[Long]("UTC_OFFSET", O.Nullable)
		def timeZone = column[String]("TIME_ZONE", O.Nullable)
		def isGeoEnabled = column[Boolean]("IS_GEO_ENABLED", O.Nullable)
		def isVerified = column[Boolean]("IS_VERIFIED", O.Nullable)
		def language = column[String]("LANGUAGE", O.Nullable)
	
		def * = id ~ name ~ screenName ~ location ~ url ~ description ~ isProtected ~ followersCount ~ friendsCount ~ listedCount ~ favouritesCount ~ statusesCount ~ createdAt ~ utcOffset ~ timeZone ~ isGeoEnabled ~ isVerified ~ language
	}
	
	object Statuses extends Table[(Long, Long, String, String, Boolean, Long, Long, String, Long, Boolean, Boolean, Long)]("STATUSES") {
		def id = column[Long]("ID", O.PrimaryKey)
		def createdAt = column[Long]("CREATED_AT", O.Nullable)
		def text = column[String]("TEXT", O.Nullable)
		def source = column[String]("SOURCE", O.Nullable)
		def isTruncated = column[Boolean]("IS_TRUNCATED", O.Nullable)
		def inReplyToStatusId = column[Long]("IN_REPLY_TO_STATUS_ID", O.Nullable)
		def inReplyToUserId = column[Long]("IN_REPLY_TO_USER_ID", O.Nullable)
		def geo = column[String]("GEO", O.Nullable)
		def retweetCount = column[Long]("RETWEET_COUNT", O.Nullable)
		def isFavorited = column[Boolean]("IS_FAVORITED", O.Nullable)
		def isRetweeted = column[Boolean]("IS_RETWEETED", O.Nullable)
		def userId = column[Long]("USER_ID")
		def user = foreignKey("USER_FK", userId, Users)(_.id)
	
		def * = id ~ createdAt ~ text ~ source ~ isTruncated ~ inReplyToStatusId ~ inReplyToUserId ~ geo ~ retweetCount ~ isFavorited ~ isRetweeted ~ userId
	}
	
	def createTables: Unit = {
		database.withSession {
			// USERS & STATUSES table
			try {
				(Users.ddl ++ Statuses.ddl).create
			} catch {
				case _:
					Exception => println("Create Table Exception.")
			}
		}
	}
	
	def insertUser(id: Long, name: String, screenName: String, location: String, url: String, description: String, isProtected: Boolean, followersCount: Long, friendsCount: Long, listedCount: Long, favouritesCount: Long, statusesCount: Long, createdAt: Long, utcOffset: Long, timeZone: String, isGeoEnabled: Boolean, isVerified: Boolean, language: String): Unit = {
		database.withSession {
			Users.insert(id, name, screenName, location, url, description, isProtected, followersCount, friendsCount, listedCount, favouritesCount, statusesCount, createdAt, utcOffset, timeZone, isGeoEnabled, isVerified, language)
		}
	}
	
	def insertStatus(id: Long, createdAt: Long, text: String, source: String, isTruncated: Boolean, inReplyToStatusId: Long, inReplyToUserId: Long, geo: String, retweetCount: Long, isFavorited: Boolean, isRetweeted: Boolean, userId: Long): Unit = {
		database.withSession {
			Statuses.insert(id, createdAt, text, source, isTruncated, inReplyToStatusId, inReplyToUserId, geo, retweetCount, isFavorited, isRetweeted, userId)
		}
	}
	
	def getUsers: Map[Long, Long] = {
		database.withSession {
			(for (user <- Users) yield user.id -> user.utcOffset).toMap
		}
	}
	
	def getStatuses: List[(Long, Long, String, Long, String)] = {
		database.withSession {
			(for (status <- Statuses.sortBy(_.id)) yield (status.id, status.userId, status.text, status.createdAt, status.geo)).list
		}
	}
	
	def getTexts: List[String] = {
		database.withSession {
			val statuses = for (status <- Statuses) yield status.text
			statuses.list
		}
	}
	
	def existUser(id: Long): Boolean = {
		database.withSession {
			val users = (for (user <- Users if user.id === id) yield user.id).list
			if (users.size == 0) false
			else true
		}
	}
	
	def existStatusForUserBetween(userId: Long, sinceId: Long, toId: Long): Boolean = {
		database.withSession {
			val statuses = (for (status <- Statuses if status.userId === userId && status.id > sinceId && status.id < toId) yield status.id).list
			if (statuses.size == 0) false
			else true
		}
	}
}