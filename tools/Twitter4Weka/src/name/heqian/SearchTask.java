/**
 * 
 */
package name.heqian;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Tweet;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

/**
 * @author heqian
 * 
 */
public class SearchTask extends TimerTask {
	static private List<Tweet> tweets = null;
	static private Query query = null;
	static private String SAVE_FILE = "./tweets.arff";

	/**
	 * 
	 */
	public SearchTask() {
		if (tweets == null) {
			tweets = new ArrayList<Tweet>();
		}
		if (query == null) {
			query = new Query();
			query.setQuery("diabetes");
			query.setLang("en");
			query.setSinceId(0);
			query.setRpp(100);
		}

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(SAVE_FILE));
			bw.write("@relation tweets\n" + "@attribute userId integer\n"
					+ "@attribute userName string\n"
					+ "@attribute text string\n" + "@data\n");
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setQuery(String query) {
		SearchTask.query.setQuery(query);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.TimerTask#run()
	 */
	@Override
	public void run() {
		Twitter twitter = TwitterFactory.getSingleton();
		QueryResult result = null;

		try {
			result = twitter.search(query);
		} catch (TwitterException e) {
			e.printStackTrace();
		}

		List<Tweet> newTweets = result.getTweets();
		tweets.addAll(newTweets);

		// save to file
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(SAVE_FILE,
					true));
			for (Tweet tweet : newTweets) {
				bw.write(tweet.getFromUserId()
						+ ","
						+ tweet.getFromUser()
						+ ",\""
						+ tweet.getText().replace('\"', ' ').replace('\'', ' ')
								.replace('\n', ' ') + "\"\n");
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// prevent duplicate tweet for next search
		query.setSinceId(result.getMaxId());

		System.out.println("["
				+ (new SimpleDateFormat("yyyy/MM/dd hh:mm:ss"))
						.format(new Date()) + "] Total Collected Tweets: "
				+ tweets.size());
	}
}
