package name.heqian.Twitter4Health

class Trainer(filePath: String) {
	var texts: List[String] = null
	
	def generateTextFile(twitterDB: TwitterDB): Unit = {
		val writer = new java.io.FileWriter(filePath, false)
		println("Fetching texts from database...")
		texts = twitterDB.getTexts
		
		println("Got texts from database. Start to write file...")
		texts foreach { text =>
			writer.write(text.replace("\n", " ") + "\n")
		}
		
		writer.close
	}
	
	def filter(wordFilePath: String): Unit = {
		val counter = scala.collection.mutable.Map.empty[String, Int]
		
		for (keyword <- scala.io.Source.fromFile(wordFilePath).getLines) {
			counter += keyword -> 0
		}
		val keywords = counter.keys
		println("Number of keywords in file \"" + wordFilePath + "\": " + keywords.size)
		
		val total = scala.io.Source.fromFile(filePath).getLines.size
		println("Size of texts before filtering: " + total)
		
		val buffer = new scala.collection.mutable.ListBuffer[String]
		var flag = false
		val tokenizer = new weka.core.tokenizers.AlphabeticTokenizer
		for (line <- scala.io.Source.fromFile(filePath).getLines) {
			flag = false
			tokenizer.tokenize(line)
			
			while (tokenizer.hasMoreElements) {
				val word: String = tokenizer.nextElement.asInstanceOf[String]
				keywords foreach { keyword =>
					if (keyword == word) {
						flag = true
						counter(word) = counter(word) + 1
					}
				}
			}
			
			if (flag) buffer += line
		}
		
		texts = buffer.toList
		println("Size of texts after filtering: " + texts.size + " (" + texts.size.toDouble / total.toDouble * 100.0 + "%)")
		counter.toList.sortBy(_._2).reverse foreach { case (word, num) =>
			println(word + " (" + num.toDouble / texts.size.toDouble * 100.0 + "%, " + num + ")")
		}
	}
}
