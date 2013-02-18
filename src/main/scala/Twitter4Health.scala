package name.heqian.Twitter4Health
import scala.slick.session.Database

object Twitter4Health {
	def main(args: Array[String]) {
		val twitterDB = new TwitterDB(Database.forURL("jdbc:h2:file:twitter", driver = "org.h2.Driver"))
		val cacheDB = new TwitterDB(Database.forURL("jdbc:h2:file:cache", driver = "org.h2.Driver"))
	
		if (args.size >= 2) {
			args(0) match {
				case "monitor" => {
					val twitterAPI = new TwitterAPI
					twitterAPI.monitorStream(twitterDB, Array[Long](), Array[String]("#" + args(1)))
				}
				case "generate" => {
					args(1) match {
						case "arff" => {
							val analyzer = new Analyzer
							analyzer.preAnalyze(twitterDB)
							analyzer.generateReport(args(1))
						}
						case "csv" => {
							val analyzer = new Analyzer
							analyzer.preAnalyze(twitterDB)
							analyzer.generateReport(args(1))
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
					if (args.size == 3) {
						thesaurus.closure(args(1), args(2).toInt)
					} else {
						thesaurus.closure(args(1), 1)
					}
				}
				case "train" => {
					val trainer = new Trainer("status.txt")
					trainer.filter(args(1))
				}
				case "text" => {
					val trainer = new Trainer(args(1))
					trainer.generateTextFile(twitterDB)
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
		println("\t\t\tgenerate [arff|csv]")
		println("\t\t\tfetch [runner] [frequency]")
		println("\t\t\tthesaurus [word] [depth]")
		println("\t\t\ttrain [word list file]")
		println("\t\t\ttext")
	}
}