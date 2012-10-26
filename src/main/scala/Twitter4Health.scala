package name.heqian.Twitter4Health

object Twitter4Health extends App {
//	val fitnessScreenNames2012 = Array("Greatist", "dailyburn", "FitBottomedGirl", "TFerriss", "bornfitness", "ElephantJournal", "AthleticFoodie", "zentofitness", "RobbWolf", "trinkfitness", "martinberkhan", "JasonFerruggia", "erwan_le_corre", "johnromaniello", "stevekamb", "tomvenuto", "iRunnerBlog", "profspiker", "TaraStiles", "lululemon", "yogadork", "RunKeeper", "MeaghanBMurphy", "Kwidrick", "Gunnar", "takeachallenge", "omgal", "kellyolexa", "bandanatraining", "Joedowdellnyc", "Sarahstanley", "bobbystrom", "jon_ptdc", "michaelpollan", "Kris_Carr", "davezinczenko", "taraparkerpope", "HealthTap", "DrWeil", "CJNutrition", "ChoosingRaw", "bradpilon", "marionnestle", "bittman", "Mark_Sisson", "guygourmet", "BodyOdd", "DrOz", "KatherineHobson", "DrEades", "MassiveHealth", "cynthiasass", "DeepakChopra", "UncleRush", "Zeo", "GretchenRubin", "brainpicker", "zen_habits", "susancain")
	
//	val twitterHelper = new TwitterHelper
		
//	twitterHelper.fetchFollowers(fitnessScreenNames2012)
//	twitterHelper.fetchStream(Array[Long](), Array[String]("#nikeplus"))
	val databaseHelper = new DatabaseHelper
	databaseHelper.generateReport
}
