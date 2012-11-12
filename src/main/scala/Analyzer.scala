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
		var retweetCounter: Long = 0
		var noDurationCounter: Long = 0
		var noOffsetCounter: Long = 0
		var withDurationAndOffsetCounter: Long = 0
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
							durationValue = duration.replace(":", "").replace("'", "").replace("\"", "").replace(".", "").toInt
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
							data(userId) += Tuple7(distanceValue, durationValue, calendar, isLocalTime, geo, id, userId)
							statusCounter += 1
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
				if (status._4 == true && status._2 != 0) withDurationAndOffsetCounter += 1
			}
		}
		var total100 = statuses.length.toDouble / 100
		println("\tUnresolved Status:\t" + unresolvedCounter + " (" + (unresolvedCounter / total100) + "%)")
		println("\tRT Status:\t" + retweetCounter + " (" + (retweetCounter / total100) + "%)")
		println("")
		println("User:\t\t\t" + userCounter + " (" + (userCounter / users.size.toDouble * 100) + "%)")
		println("Status:\t\t\t" + statusCounter + " (" + (statusCounter / total100) + "%)")
		total100 = statusCounter.toDouble / 100
		println("\tWith Duration Data:\t" + (statusCounter - noDurationCounter) + " (" + ((statusCounter - noDurationCounter) / total100) + "%)")
		println("\tWith UTC Offset Data:\t" + (statusCounter - noOffsetCounter) + " (" + ((statusCounter - noOffsetCounter) / total100) + "%)")
		println("\t\tWith Duration & UTC Offset Data:\t" + withDurationAndOffsetCounter + " (" + (withDurationAndOffsetCounter / total100) + "%)")
		println("\tWith Geo Location Data:\t" + (statusCounter - noGeoCounter) + " (" + ((statusCounter - noGeoCounter) / total100) + "%)")
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

	def generateReport(fileType: String, withDuration: Boolean, withUTC: Boolean): Unit = {
		// Generate running report
		var writer: FileWriter = null
		
		fileType match {
			case "csv" => {
				writer = new FileWriter("running.csv", false)
				writer.write("distance (km),duration (min),pace (km/h),dayOfWeek,hourOfDay,nextRunning (day)\n")
			}
			case "arff" => {
				writer = new FileWriter("running.arff", false)
				writer.write("@RELATION running\n")
				writer.write("@ATTRIBUTE distance NUMERIC\n")
				writer.write("@ATTRIBUTE duration NUMERIC\n")
				writer.write("@ATTRIBUTE pace NUMERIC\n")
				writer.write("@ATTRIBUTE dayOfWeek {1,2,3,4,5,6,7}\n")
				writer.write("@ATTRIBUTE hourOfDay {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23}\n")
				writer.write("@ATTRIBUTE nextRunning NUMERIC\n")
				writer.write("\n@DATA\n")
			}
			case _ => {
				println("Unsupported output file type: " + fileType)
				return
			}
		}
		data.values.foreach {value =>
			for (i <- 0 until value.size) {
				val status = value(i)
				val distance: Double = status._1		// km
				val duration: Double = status._2 / 60.0	// min
				var pace: Double = 0					// km/h
				var dayOfWeek: Int = 0
				var hourOfDay: Int = 0
				var nextRunning: Double = 0
				
				if ((withDuration && duration == 0) || (withUTC && !status._4)) {
				} else {
					writer.write(distance + ",")
					if (duration == 0) {
						writer.write("?,?,")
					} else {
						pace = distance / duration * 60.0
						writer.write(duration + "," + pace + ",")
					}
				
					if (status._4) {
						dayOfWeek = status._3.get(Calendar.DAY_OF_WEEK)
						hourOfDay = status._3.get(Calendar.HOUR_OF_DAY)
						writer.write(dayOfWeek + "," + hourOfDay + ",")
					} else {
						writer.write("?,?,")
					}
				
					if (i == value.size - 1) {
						writer.write("?\n")
					} else {
						nextRunning = (value(i + 1)._3.getTimeInMillis - value(i)._3.getTimeInMillis) / 1000.0 / 3600.0 / 24.0
						writer.write(nextRunning + "\n")
					}
				}
			}
		}
		writer.close
		
		// Generate user report
		fileType match {
			case "csv" => {
				writer = new FileWriter("user.csv", false)
				writer.write("times,frequency (times/week)\n")
			}
			case "arff" => {
				writer = new FileWriter("user.arff", false)
				writer.write("@RELATION user\n")
				writer.write("@ATTRIBUTE times NUMERIC\n")
				writer.write("@ATTRIBUTE frequency NUMERIC\n")
				writer.write("\n@DATA\n")
			}
			case _ => {
				println("Unsupported output file type: " + fileType)
				return
			}
		}
		data.values.foreach {value =>
			var frequency: Double = 0
			
			writer.write(value.size + ",")
			
			if (value.size > 1) {
				frequency = (value.size - 1) / ((value(value.size - 1)._3.getTimeInMillis - value(0)._3.getTimeInMillis) / 1000.0 / 3600.0 / 24.0 / 7.0)
				writer.write(frequency + "\n")
			} else {
				writer.write("?\n")
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
