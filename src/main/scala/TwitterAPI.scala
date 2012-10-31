package name.heqian.Twitter4Health

import scala.collection.JavaConverters._
import scala.collection.mutable
import twitter4j._
import twitter4j.conf.ConfigurationBuilder
import java.io._

class TwitterAPI {
	private val configuration = {
		val configurationBuilder = new ConfigurationBuilder
		configurationBuilder
			.setDebugEnabled(true)
			.setOAuthConsumerKey("IyOGRY69CQr5Ral53b0vvQ")
			.setOAuthConsumerSecret("1wAHDsqKWImb1sI2b4Ka7xmsLuPoqIvwtNMYUx8j3Rs")
			.setOAuthAccessToken("17567078-qjclxyHJ86TYK1Y56DSvbgIj5ToRVzl7sAHPwP9WQ")
			.setOAuthAccessTokenSecret("vrPpbAvjX1LoNatxMcpN2mYXMbnd3NEwrYl9u96A")
			.build
		}
	private val twitter = new TwitterFactory(configuration).getInstance
	private val stream = new TwitterStreamFactory(configuration).getInstance
//	private val writer = new FileWriter("users.txt", true)
	
	def fetchFollowers(screenNames: Array[String]): Unit = {
		var userSet = mutable.Set.empty[Long]
		for (screenName <- screenNames) {
			var cursor: Long = -1
			var hasNext = true
			while(hasNext) {
				val ids = twitter.getFollowersIDs(screenName, cursor)
				if (ids.getRateLimitStatus.getRemainingHits > 0) {
					ids.getIDs.foreach(userSet.add)
					ids.getIDs.foreach(println)
					hasNext = ids.hasNext
					if (hasNext) cursor = ids.getNextCursor
				} else {
					println("Feteched " + userSet.size + " users")
					println("API Limit Reset in: " + ids.getRateLimitStatus.getSecondsUntilReset / 60 + " mins")
					Thread.sleep((ids.getRateLimitStatus.getSecondsUntilReset + 10) * 1000)
				}
			}
		}
		
//		userSet.foreach(id => writer.write(id.toString + "\n"))
	}
	
	def monitorStream(database: TwitterDB, follow: Array[Long], track: Array[String]): Unit = {
		//var userSet = mutable.Set.empty[Long]
	
		database.createTables
		
		val listener = new StatusListener {
			def onStatus(status: Status) = {
				/*
				userSet.add(status.getUser.getId)
				println("@" + status.getUser.getScreenName + " - " + status.getText)
				println("Total: " + userSet.size)
				writer.write(status.getUser.getId.toString + "\n")
				writer.flush
				*/
				val user = status.getUser
				var url = ""
				if (user.getURL != null) url = user.getURL.toString
				try {
					database.insertUser(
						user.getId,
						user.getName,
						user.getScreenName,
						user.getLocation,
						url,
						user.getDescription,
						user.isProtected,
						user.getFollowersCount,
						user.getFriendsCount,
						user.getListedCount,
						user.getFavouritesCount,
						user.getStatusesCount,
						user.getCreatedAt.getTime,
						user.getUtcOffset,
						user.getTimeZone,
						user.isGeoEnabled,
						user.isVerified,
						user.getLang
					)
				} catch {
					case e: Exception => println("Insert User Exception: " + e)
				}
				
				var geo = ""
				if (status.getGeoLocation != null) geo = status.getGeoLocation.toString
				try {
					database.insertStatus(
						status.getId,
						status.getCreatedAt.getTime,
						status.getText,
						status.getSource,
						status.isTruncated,
						status.getInReplyToStatusId,
						status.getInReplyToUserId,
						geo,
						status.getRetweetCount,
						status.isFavorited,
						status.isRetweet,
						status.getUser.getId
					)
				} catch {
					case e: Exception => println("Insert Status Exception: " + e)
				}
				
				println("@" + status.getUser.getScreenName + " - " + status.getText)
			}
	
			def onScrubGeo(userId: Long, upToStatusId: Long) = {
				println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId)
			}
			def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) = {
				println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId())
			}
	
			def onTrackLimitationNotice(numberOfLimitedStatuses: Int) = {
				println("Got track limitation notice:" + numberOfLimitedStatuses)
			}
	
			def onException(exception: Exception) = {
				exception.printStackTrace
			}
		}
		
		stream.addListener(listener)
		stream.filter(new FilterQuery(0, follow, track))
	}
	
	def fetchUserTimeline(database: TwitterDB, userId: Long, sinceId: Long, toId: Long): Unit = {
		val paging = new Paging(1, 200, sinceId, toId)
		var hasNext = true
		
		// Check if the statuses is already in database
		if (database.existStatusForUserBetween(userId, sinceId, toId)) {
			println("\tAlready cached statuses for user - [" + userId + "] between [" + sinceId +"] <-> [" + toId + "]")
			return
		} else {
			println("\tFeteching statuses for user - [" + userId + "] between [" + sinceId +"] <-> [" + toId + "]")
		}
		
		// Check if the user is already in database
		if (! database.existUser(userId)) {
			database.insertUser(userId, "", "", "", "", "", false, 0, 0, 0, 0, 0, 0, 0, "", false, false, "")
		}
		
		while (hasNext) {
			val statuses = twitter.getUserTimeline(userId, paging)
			
			// API limit reached
			if (statuses.getRateLimitStatus.getRemainingHits == 0) {
				val mins = statuses.getRateLimitStatus.getSecondsUntilReset / 60 + 1
				for (i <- 0 until mins) {
					println("\n******* API Limit Reset in: " + (mins - i) + " mins *******\n")
					Thread.sleep(60000)	// 1 min
				}
			}
			
			// Still have API quota
			if (statuses.size == 0) {
				hasNext = false
			} else {
				var startIndex = 0
				if (statuses.get(0).getId == toId) {	// Reached the "toId" statuses
					hasNext = false
					startIndex = 1
				} else {
					paging.setSinceId(statuses.get(0).getId)	// Set the last status (Twitter return reverse order) as the "sinceId" status
					hasNext = true
					startIndex = 0
				}
				
				for (i <- startIndex until statuses.size) {
					val status = statuses.get(i)
					println("\t\tFetched (" + (i + 1) + "/" + statuses.size + "): [" + status.getId + "] - " + status.getText)
					
					var geo = ""
					if (status.getGeoLocation != null) geo = status.getGeoLocation.toString
					try {
						database.insertStatus(
							status.getId,
							status.getCreatedAt.getTime,
							status.getText,
							status.getSource,
							status.isTruncated,
							status.getInReplyToStatusId,
							status.getInReplyToUserId,
							geo,
							status.getRetweetCount,
							status.isFavorited,
							status.isRetweet,
							status.getUser.getId
						)
					} catch {
						case e: Exception => println("Insert Status Exception: " + e)
					}
				} 
			}
		}
	}
}
