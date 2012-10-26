package name.heqian.Twitter4Health

import scala.collection.mutable
import scala.slick.driver.H2Driver.simple._
import Database.threadLocalSession
import java.io._
import java.util.Calendar

class DatabaseHelper {
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
	
	val database = Database.forURL("jdbc:h2:file:twitter", driver = "org.h2.Driver")
	
	def createTables = {
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
	
	def insertUser(id: Long, name: String, screenName: String, location: String, url: String, description: String, isProtected: Boolean, followersCount: Long, friendsCount: Long, listedCount: Long, favouritesCount: Long, statusesCount: Long, createdAt: Long, utcOffset: Long, timeZone: String, isGeoEnabled: Boolean, isVerified: Boolean, language: String) = {
		database.withSession {
			Users.insert(id, name, screenName, location, url, description, isProtected, followersCount, friendsCount, listedCount, favouritesCount, statusesCount, createdAt, utcOffset, timeZone, isGeoEnabled, isVerified, language)
		}
	}
	
	def insertStatus(id: Long, createdAt: Long, text: String, source: String, isTruncated: Boolean, inReplyToStatusId: Long, inReplyToUserId: Long, geo: String, retweetCount: Long, isFavorited: Boolean, isRetweeted: Boolean, userId: Long) = {
		database.withSession {
			Statuses.insert(id, createdAt, text, source, isTruncated, inReplyToStatusId, inReplyToUserId, geo, retweetCount, isFavorited, isRetweeted, userId)
		}
	}
	
	def generateReport = {
		database.withSession {
			// Get all
			val statuses = (for (status <- Statuses) yield (status.id, status.userId, status.text, status.createdAt)).list
			val users = (for (user <- Users) yield (user.id -> user.utcOffset)).toMap
			println("Total User:\t\t" + users.size)
			println("Total Status:\t\t" + statuses.length)
			
			// Filter status & generate (userId -> List((distance, duration, date, isLocalTime)))
			val data = mutable.Map.empty[Long, mutable.ListBuffer[(Double, Int, Calendar, Boolean)]]
			
			var unresolvedCounter: Long = 0
			var duplicateCounter: Long = 0
			var retweetCounter: Long = 0
			
			val nikePattern = """^(.*)I (just )?(finished|crushed) a (\d+.\d+) ?(km|mi) run  ?(with a pace of (\d+'\d+).* )?(with a time of (.+) )?with (.+)\..*$""".r
			statuses foreach { case (id, userId, text, createdAt) =>
				text.replace("\n", "") match {
					case nikePattern(comment, word1, word2, distance, unit, paceSentence, pace, durationSentence, duration, client) => {
						// Check if the tweet is retweeted by someone
						if (comment.contains("RT") || comment.contains("\"@")) {
							retweetCounter += 1
						} else {
							// Distance
							var distanceValue: Double = distance.replace(",", ".").replace(":", ".").toDouble
							if (unit == "mi") {
								// Convert to km
								distanceValue = distanceValue * 1.609344
							}
							// Duration
							var durationValue: Int = 0
							if (pace != null) {
								durationValue = pace.replace("'", "").toInt
								durationValue = durationValue / 100 * 60 + durationValue % 100
								durationValue = (durationValue * distance.replace(",", ".").toDouble).toInt
								// println(pace + " == " + durationValue)
							} else if (duration != null) {
								durationValue = duration.replace(":", "").replace("'", "").replace("\"", "").toInt
								durationValue = durationValue / 10000 * 3600 + durationValue / 100 % 100 * 60 + durationValue % 100
								// println(time + " == " + durationValue)
							} else {
								// noDurationCounter += 1
							}
							// Start Time
							var offset = users(userId)
							var isLocalTime = true
							if (offset == -1) {
								isLocalTime = false
								offset = 0
							}
							var calendar = Calendar.getInstance()
							calendar.setTimeInMillis(createdAt + offset * 1000 - calendar.get(Calendar.ZONE_OFFSET) - calendar.get(Calendar.DST_OFFSET))
						
							// Add to dataset
							if (data.contains(userId)) {
								val last = data(userId).last
								// Check duplicate
								if (last._1 == distanceValue && last._2 == durationValue) {
									duplicateCounter += 1
								} else {
									data(userId) += Tuple4(distanceValue, durationValue, calendar, isLocalTime)
								}
							} else {
								data += userId -> mutable.ListBuffer(Tuple4(distanceValue, durationValue, calendar, isLocalTime))
							}
						}
					}
					case _ => {
						// if (text.contains("crushed") || text.contains("finished"))
							unresolvedCounter += 1
					}
				}
			}
			
			// Summary
			var sizeOfData = 0
			var noDurationCounter: Long = 0
			var noOffsetCounter: Long = 0
			data.values.foreach { value =>
				sizeOfData += value.size
				value.foreach { status => 
					if (status._2 == 0) noDurationCounter += 1
					if (status._4 == false) noOffsetCounter += 1
				}
			}
			println("\tUnresolved Status:\t" + unresolvedCounter)
			println("\tDuplicate Status:\t" + duplicateCounter)
			println("\tRT Status:\t" + retweetCounter)
			println("")
			println("User:\t\t\t" + data.size)
			println("Status:\t\t\t" + sizeOfData)
			println("\tNo Duration Data:\t" + noDurationCounter)
			println("\tNo UTC Offset Data:\t" + noOffsetCounter)
			println("")
			
			// Generate .csv
			var writer = new FileWriter("user.csv", false)
			writer.write("frequency, distance, duration, interval\n")
			data.values.foreach {value =>
				var distance: Double = 0
				var duration: Int = 0
				value.foreach {status => 
					distance += status._1
					duration += status._2
				}
				var interval: Double = 0
				if (value.size > 1) {
					var intervalCounter = 0
					for (i <- 1 until value.size) {
						intervalCounter += 1
						interval += value(i)._3.getTimeInMillis - value(i - 1)._3.getTimeInMillis
					}
					if (intervalCounter > 1) {
						interval /= intervalCounter
					}
				}
				writer.write(value.size + ", " + distance / value.size + ", " + duration / 60.0 / value.size + ", " + interval / 1000 / 3600 / 24 + "\n")
			}
			writer.close
			
			writer = new FileWriter("status.csv", false)
			writer.write("distance, duration, pace, dayOfWeek, hourOfDay\n")
			data.values.foreach {value =>
				value.foreach {status =>
					val distance = status._1
					val duration = status._2
					var pace: Double = 0
					var dayOfWeek = -1
					var hourOfDay = -1
					if (status._4) {
						dayOfWeek = status._3.get(Calendar.DAY_OF_WEEK)
						hourOfDay = status._3.get(Calendar.HOUR_OF_DAY)
					}
					if (duration != 0) pace = distance / duration * 1000 * 3.6
					writer.write(distance + ", " + duration / 60.0 + ", " + pace + ", " + dayOfWeek + ", " + hourOfDay + ", " + "\n")
				}
			}
			writer.close
		}
	}
}