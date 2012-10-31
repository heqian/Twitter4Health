package name.heqian.Twitter4Health

import scala.collection.mutable
import java.io._
import java.util.Calendar
import java.util.TimeZone

class Analyzer {
	// Filter status & generate (userId -> List((distance, duration, utcTime, isLocalTime, geo, statusId, userId)))
	val data = mutable.Map.empty[Long, mutable.ListBuffer[(Double, Int, Calendar, Boolean, String, Long, Long)]]
	
	def preAnalyze(twitterDB: TwitterDB): Unit = {
		// Get all
		val statuses = twitterDB.getStatuses
		println("Total Status:\t\t" + statuses.length)
		val users = twitterDB.getUsers
		println("Total User:\t\t" + users.size)
			
		var unresolvedCounter: Long = 0
		var duplicateCounter: Long = 0
		var retweetCounter: Long = 0
		var noDurationCounter: Long = 0
		var noOffsetCounter: Long = 0
		var noGeoCounter: Long = 0
		var userCounter: Long = 0
		var statusCounter: Long = 0
			
		val nikePattern = """^(.*)I (just )?(finished|crushed) a (\d+.\d+) ?(km|mi) run  ?(with a pace of (\d+'\d+).* )?(with a time of (.+) )?with (.+)\..*$""".r
		statuses foreach { case (id, userId, text, createdAt, geo) =>
			text.replace("\n", "") match {
				case nikePattern(comment, word1, word2, distance, unit, paceSentence, pace, durationSentence, duration, client) => {
					// Check if the tweet is retweeted by someone
					if (comment.contains("RT") || comment.contains("\"@")) {
						retweetCounter += 1
					} else {
						// Find the user info
						val user = users(userId)
						
						// Distance
						var distanceValue: Double = distance.replace(",", ".").replace(":", ".").toDouble
						if (unit == "mi") {
							// Convert to km
							distanceValue = distanceValue * 1.609344
						}
						// Duration
						var durationValue: Int = 0
						if (duration != null) {
							durationValue = duration.replace(":", "").replace("'", "").replace("\"", "").toInt
							durationValue = durationValue / 10000 * 3600 + durationValue / 100 % 100 * 60 + durationValue % 100
							// println(time + " == " + durationValue)
						} else if (pace != null) {
							durationValue = pace.replace("'", "").toInt
							durationValue = durationValue / 100 * 60 + durationValue % 100
							durationValue = (durationValue * distance.replace(",", ".").toDouble).toInt
							// println(pace + " == " + durationValue)
						}
							
						// Created Time
						var offset: Long = user
						var isLocalTime = true
						if (offset == -1) {
							isLocalTime = false
							offset = 0
						}
						var calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
						calendar.setTimeInMillis(createdAt + offset * 1000)
						
						// Geo location
						
						// Add to dataset
						if (data.contains(userId)) {
							val last = data(userId).last
							// Check duplicate
							if (last._1 == distanceValue && last._2 == durationValue) {
								duplicateCounter += 1
							} else {
								data(userId) += Tuple7(distanceValue, durationValue, calendar, isLocalTime, geo, id, userId)
								statusCounter += 1
							}
						} else {
							data += userId -> mutable.ListBuffer(Tuple7(distanceValue, durationValue, calendar, isLocalTime, geo, id, userId))
							userCounter += 1
							statusCounter += 1
						}
					}
				}
				case _ => {
					// if (text.contains("crushed") || text.contains("finished"))
						unresolvedCounter += 1
				}
			}
		}
		
		// Print summary
		data.values.foreach { user =>
			user.foreach { status => 
				if (status._2 == 0) noDurationCounter += 1
				if (status._4 == false) noOffsetCounter += 1
				if (status._5 == "") noGeoCounter += 1
			}
		}
		println("\tUnresolved Status:\t" + unresolvedCounter)
		println("\tDuplicate Status:\t" + duplicateCounter)
		println("\tRT Status:\t" + retweetCounter)
		println("")
		println("User:\t\t\t" + userCounter)
		println("Status:\t\t\t" + statusCounter)
		println("\tNo Duration Data:\t" + noDurationCounter)
		println("\tNo UTC Offset Data:\t" + noOffsetCounter)
		println("\tNo Geo Location Data:\t" + noGeoCounter)
		println("")
	}
	
	def generateHealthDB(healthDB: HealthDB): Unit = {
		healthDB.createTables
		healthDB.database.withSession {
			data.values.foreach { value =>
				value.foreach { status => 
					healthDB.insertRunning(status._6, status._7, status._1, status._2, status._3.getTimeInMillis, status._4, status._5)
				}
			}
		}
	}

	def generateCSV(): Unit = {
		// Generate .csv
		var writer = new FileWriter("user.csv", false)
		writer.write("frequency, distance (km), duration (min), interval (day)\n")
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
		writer.write("distance (km), duration (min), pace (km/h), dayOfWeek, hourOfDay\n")
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
	
	def fetchTweetsBetween2Runnings(cacheDB: TwitterDB, frequency: Int) = {
		cacheDB.createTables
		
		val twitterAPI = new TwitterAPI
		var counter: Long = 0
		var total: Long = 0
		
		// Pre-analyze the size of data
		data.values.foreach { runner =>
			if (runner.size >= frequency) {
				total += 1
			}
		}
		
		data.values.foreach { runner =>
			if (runner.size >= frequency) {
				println("Fetching for user: " + runner(0)._7)
				for (i <- 1 until runner.size) {
					var sinceId = runner(i - 1)._6
					var toId = runner(i)._6
					twitterAPI.fetchUserTimeline(cacheDB, runner(0)._7, sinceId, toId)
				}
				counter += 1
				println("Completed [" + counter + "/" + total + "]: " + counter.toFloat / total.toFloat * 100.0 + "%\n")
			}
		}
	}
}
