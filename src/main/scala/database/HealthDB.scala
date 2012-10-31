package name.heqian.Twitter4Health

import scala.slick.driver.H2Driver.simple._
import Database.threadLocalSession

class HealthDB(val database: Database) {
	object Runnings extends Table[(Long, Long, Double, Int, Long, Boolean, String)]("RUNNINGS") {
		def statusId = column[Long]("STATUS_ID", O.PrimaryKey)
		def userId = column[Long]("USER_ID", O.NotNull)
		def distance = column[Double]("DISTANCE", O.NotNull)			// km
		def duration = column[Int]("DURATION", O.Nullable)				// second
		def createdAt = column[Long]("CREATED_AT", O.NotNull)			// UTC
		def isLocalTime = column[Boolean]("IS_LOCAL_TIME", O.NotNull)	// UTC
		def location = column[String]("LOCATION", O.Nullable)
	
		def * = statusId ~ userId ~ distance ~ duration ~ createdAt ~ isLocalTime ~ location
	}
	
	def createTables: Unit = {
		database.withSession {
			// RUNNINGS table
			try {
				Runnings.ddl.create
			} catch {
				case _:
					Exception => println("Create Table Exception.")
			}
		}
	}
	
	def insertRunning(statusId: Long, userId: Long, distance: Double, duration: Int, createdAt: Long, isLocalTime: Boolean, location: String): Unit = {
		database.withSession {
			Runnings.insert(statusId, userId, distance, duration, createdAt, isLocalTime, location)
		}
	}
}