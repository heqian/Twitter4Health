package name.heqian.Twitter4Health
import scala.slick.session.Database

object Twitter4Health {
	def main(args: Array[String]) {
		val twitterDB = new TwitterDB(Database.forURL("jdbc:h2:file:twitter", driver = "org.h2.Driver"))
		val cacheDB = new TwitterDB(Database.forURL("jdbc:h2:file:cache", driver = "org.h2.Driver"))
		val healthDB = new HealthDB(Database.forURL("jdbc:h2:file:health", driver = "org.h2.Driver"))
	
		if (args.size >= 2) {
			args(0) match {
				case "monitor" => {
					val twitterAPI = new TwitterAPI
					twitterAPI.monitorStream(twitterDB, Array[Long](), Array[String]("#" + args(1)))
				}
				case "generate" => {
					val analyzer = new Analyzer
					args(1) match {
						case "arff" => {
							analyzer.preAnalyze(twitterDB)
							if (args.size == 2) {
								analyzer.generateReport(args(1), false, false)
							} else if (args.size == 3) {
								args(2) match {
									case "duration" => analyzer.generateReport(args(1), true, false)
									case "utc" => analyzer.generateReport(args(1), false, true)
									case _ => showCmdDescription
								}
							} else if (args.size == 4) {
								if ((args(2) == "duration" && args(3) == "utc") || (args(3) == "duration" && args(2) == "utc")) {
									analyzer.generateReport(args(1), true, true)
								} else {
									showCmdDescription
								}
							}
						}
						case "csv" => {
							analyzer.preAnalyze(twitterDB)
							if (args.size == 2) {
								analyzer.generateReport(args(1), false, false)
							} else if (args.size == 3) {
								args(2) match {
									case "duration" => analyzer.generateReport(args(1), true, false)
									case "utc" => analyzer.generateReport(args(1), false, true)
									case _ => showCmdDescription
								}
							} else if (args.size == 4) {
								if ((args(2) == "duration" && args(3) == "utc") || (args(3) == "duration" && args(2) == "utc")) {
									analyzer.generateReport(args(1), true, true)
								} else {
									showCmdDescription
								}
							}
							
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
				case "thesaurus" => {
					val thesaurus = new MerriamWebsterAPI
					thesaurus.closure(args(1), 2)
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
		println("\t\t\tgenerate [arff|csv [duration|utc]]|healthdb]")
		println("\t\t\tfetch [runner] [frequency]")
	}
}