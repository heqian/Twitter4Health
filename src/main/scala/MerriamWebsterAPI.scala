package name.heqian.Twitter4Health

import scala.collection.mutable
import scala.xml.XML
import java.io._
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

class MerriamWebsterAPI {
	val baseUrl = "http://www.dictionaryapi.com/api/v1/references/thesaurus/xml/"
	val apiKey = "5d4087e5-ca85-46c1-8c79-af26aa7756e2"
	
	def getApiUrl(word: String): String = {
		baseUrl + word.replace(" ", "%20") + "?key=" + apiKey
	}
	
	def getContent(url:String): String = {
		val httpClient = new DefaultHttpClient()
		val httpResponse = httpClient.execute(new HttpGet(url))
		val entity = httpResponse.getEntity()
		var content = ""
		
		if (entity != null) {
			val inputStream = entity.getContent()
			content = io.Source.fromInputStream(inputStream).getLines.mkString
			inputStream.close
		}
		
		httpClient.getConnectionManager().shutdown()
		content
	}

	def getSynonymsAndRelatedWords(word: String): mutable.Set[String] = {
		val wordSet = mutable.Set.empty[String]
		val url = getApiUrl(word)
		val content = getContent(url)
		val xml = XML.loadString(content)
		
		val syn = xml \\ "syn"
		val rel = xml \\ "rel"
		val nodes = syn ++ rel
		
		println("\"" + word + "\" has " + nodes.size / 2 + " meanings.")
		
		for (node <- nodes) {
			val words = node.text.split("(,|;) ")
			words.foreach { w =>
				var wd = w
				if (wd.contains(" [")) wd = wd.split(" \\[")(0)
				
				if (wd.contains(" (also ")) {
					wd.replace(")", "").split(" \\(also ").foreach(wordSet.add)
				} else if (wd.contains(" (or ")) {
					wd.replace(")", "").split(" \\(or ").foreach(wordSet.add)
				} else if (wd.contains(" (")) {
					val parts = wd.replace(")", "").split(" \\(")
					wordSet += parts(0)
					parts(1).split(" or ").foreach { part =>
						wordSet += parts(0) + " " + part
					}
				} else {
					if (wd.contains("(")) {
						wordSet += wd.split("\\(")(0)
					} else {
						wordSet += wd
					}
				}
			}
		}
		
		wordSet
	}
	
	def closure(word: String, digLevel: Int): Unit = {
		val wordSet = mutable.Set.empty[String]
		val waitingList = new mutable.ListBuffer[String]
		waitingList += word
		wordSet += word
		var lastWordForCurrentLevel = word	// mutable.Set doesn't have size function, sad...
		var level = digLevel
		
		while (! waitingList.isEmpty && level > 0) {
			// Pop first word
			val waitingWord = waitingList.head
			waitingList.trimStart(1)
			
			// Fetch word set
			val results = getSynonymsAndRelatedWords(waitingWord)
			// Add
			results.foreach { result =>
				if (! wordSet.contains(result)) {
					waitingList += result
					wordSet += result
				}
			}
			
			// Update level counter
			if (waitingWord == lastWordForCurrentLevel) {
				level -= 1
				lastWordForCurrentLevel = waitingList.last
				
				// Print the result of this level
				println("Level " + digLevel - level + ": " + waitingList.size)
				println("\t" + waitingList)
			}
		}
		
		val result = wordSet.result
		println("Word Set for \"" + word + "\": " + result.size)
		println("\t" + result)
	}
}
