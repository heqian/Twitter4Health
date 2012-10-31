package name.heqian.Twitter4Health
import scala.slick.session.Database

object Twitter4Health {
	def main(args: Array[String]) {
		val twitterDB = new TwitterDB(Database.forURL("jdbc:h2:file:twitter", driver = "org.h2.Driver"))
		val cacheDB = new TwitterDB(Database.forURL("jdbc:h2:file:cache", driver = "org.h2.Driver"))
		val healthDB = new HealthDB(Database.forURL("jdbc:h2:file:health", driver = "org.h2.Driver"))
	
		if (args.size > 2) {
			args(0) match {
				case "monitor" => {
					val twitterAPI = new TwitterAPI
					twitterAPI.monitorStream(twitterDB, Array[Long](), Array[String]("#" + args(1)))
				}
				case "generate" => {
					val analyzer = new Analyzer
					args(1) match {
						case "csv" => {
							analyzer.preAnalyze(twitterDB)
							analyzer.generateCSV
						}
						case "healthdb" => {
							analyzer.preAnalyze(twitterDB)
							analyzer.generateHealthDB(healthDB)
						}
						case _ => showCmdDescription
					}
				}
				case "fetch" => {
					val analyzer = new Analyzer
					args(1) match {
						case "runner" => {
							analyzer.preAnalyze(twitterDB)
							var frequency = 2
							if (args.size == 3) frequency = args(2).toInt
							analyzer.fetchTweetsBetween2Runnings(cacheDB, frequency)
						}
						case _ => showCmdDescription
					}
				}
				case _ => showCmdDescription
			}
		} else {
			showCmdDescription
		}
	}
	
	def showCmdDescription() = {
		println("Usage:")
		println("\tTwitter4Health")
		println("\t\t\tmonitor [hashtag]")
		println("\t\t\tgenerate [csv|healthdb]")
		println("\t\t\tfetch [runner] [frequency]")
	}
}