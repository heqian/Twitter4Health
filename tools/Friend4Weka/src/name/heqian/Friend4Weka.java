package name.heqian;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import twitter4j.IDs;
import twitter4j.RateLimitStatus;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class Friend4Weka {

	/**
	 * @param args
	 * @throws IOException
	 * @throws TwitterException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException {
		String FILE_LOCATION = "./tweets.csv";
		int userIdIndex = 0;
		int numOfAttributes = 0;

		String header = null;

		if (args.length == 2) {
			userIdIndex = Integer.valueOf(args[0]).intValue();
			FILE_LOCATION = args[1];
		}

		// File
		BufferedReader reader = new BufferedReader(
				new FileReader(FILE_LOCATION));
		BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_LOCATION
				+ ".csv"));

		header = reader.readLine();
		numOfAttributes = header.split(",").length;
		writer.write(header + "\n");

		// Twitter
		Twitter twitter = TwitterFactory.getSingleton();

		// each line of data is a instance
		ArrayList<String> data = new ArrayList<String>();
		String line = null;
		while ((line = reader.readLine()) != null)
			data.add(line);

		// create new data
		for (int i = 0; i < data.size(); i++) {
			RateLimitStatus status = null;
			try {
				status = twitter.getRateLimitStatus();
			} catch (TwitterException e) {
				e.printStackTrace();
			}
			if (status.getRemainingHits() < 1) {
				System.out.println("Sleep " + status.getSecondsUntilReset()
						+ " seconds for Twitter to rest API limit. [i=" + i
						+ "]");
				writer.close();
				Thread.sleep((long) (status.getSecondsUntilReset() + 10) * 1000);
				writer = new BufferedWriter(new FileWriter(FILE_LOCATION
						+ ".csv", true));

			}
			long userA = Long.valueOf(data.get(i).split(",")[0]).longValue();
			long[] friendListOfA = null;
			try {
				friendListOfA = twitter.getFriendsIDs(userA, -1).getIDs();
			} catch (TwitterException e) {
				e.printStackTrace();
			}
			for (int j = 0; j < data.size(); j++) {
				if (i != j) {
					long userB = Long.valueOf(data.get(j).split(",")[0])
							.longValue();
					boolean isFriend = false;

					if (friendListOfA == null) {
						isFriend = false;
					} else {
						for (int k = 0; k < friendListOfA.length; k++) {
							if (friendListOfA[k] == userB)
								isFriend = true;
						}
					}
					
					String newLine = data.get(i) + "," + data.get(j);
					if (isFriend) {
						System.out.println(i + " >>> " + j);
						writer.write(newLine + ",1\n");
					} else {
						writer.write(newLine + ",0\n");
						System.out.println(i + " xxx " + j);
					}
				}
			}
		}

		reader.close();
		writer.close();
	}
}
