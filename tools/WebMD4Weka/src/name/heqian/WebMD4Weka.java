/**
 * 
 */
package name.heqian;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;

/**
 * @author heqian
 * 
 */
public class WebMD4Weka {
	static private String SAVE_FILE = "./webmd.arff";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String urlString = "";
		String googleReaderUrlString = "http://www.google.com/reader/atom/feed/";

		// Check command line parameters
		switch (args.length) {
		case 3:
			if (args[0].equals("y"))
				urlString = googleReaderUrlString + args[2] + "?r=n&n="
						+ args[1];
			else
				System.out.println("Usage WebMD4Weka.jar [y/n [num]] [url]");
			break;
		case 2:
			if (args[0].equals("n"))
				urlString = args[2];
			else
				System.out.println("Usage WebMD4Weka.jar [y/n [num]] [url]");
			break;
		case 1:
			urlString = args[2];
			break;
		default:
			urlString = googleReaderUrlString
					+ "http://rssfeeds.webmd.com/rss/rss.aspx%3Fcid=1033"
					+ "?r=n&n=10000";
			System.out.println("Usage WebMD4Weka.jar [y/n [num]] [url]");
			System.out
					.println("By default, fetching: \"http://rssfeeds.webmd.com/rss/rss.aspx?cid=1033\" with Google Reader API...");
			break;
		}

		// Construct connection
		URL url = null;
		try {
			url = new URL(urlString);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}

		URLConnection connection = null;
		try {
			connection = url.openConnection();
		} catch (IOException e) {
			e.printStackTrace();
		}
		connection.setRequestProperty("User-Agent", "Evil Google");
		connection
				.setRequestProperty(
						"Cookie",
						"GRLD=UNSET:07126809492836610140; SID=DQAAAOAAAAD8x0zbBA9szAlIxkyB56wIY3Dv2RgoGCQlxLy_xlUNVZkiGIzvAvhwZlRzOCBf98ccwnQ19hbQ7m20DWme-jwQ_DM1QEmyxI9ZfOc8K76C5-8qkI8CvWk3DXJZqY0RE6QKuzeyaFihyWSsBMbgw_FGYTJwJ6_8bSqh0kYNoQcPXxBxE_9QfV0zt69_xG8K_K96iar5yMDmcmu4oDwKwnUHSU_SchoySqPUNkvnXb7iSTRfJ4J_yiwUR3Mn0QDgOfeuRajm5OLjkNn_jrVnruBHg1nsih1BQLOIznb3FF9C4A; NID=59=hVf57wlXFECs0embUwuFfLs3iR5Won3QRGmT-cjsliqVq5-Sa46Tipw4rqSbG0gt9dj54w2FjEcL5ZEfiDBAnc4iZDmXi03GlBPf1ER1nMgqi9utvXNvpFIrdms6Jgw3_-mao7Q1SwVOXvP0YnK2U3KWdOEgEJTNC24a8HrxAaECRvaRlhkBlx_Flg9ak6vyAYIdJemGSBdKlmRU; APISID=dz8Trdtq1_gUt8ev/AkFudiX1TJTF723V-; HSID=AF-9mXgl-r4Q77qHM; PREF=ID=ad97afa7bf0c3e52:U=7ca6bc1a13a09ce7:FF=0:TM=1333761396:LM=1333821749:GM=1:S=_hbDk8EanIIkW2V4");

		// Fetch items from Atom feed
		SyndFeed feed = null;
		try {
			feed = new SyndFeedInput().build(new XmlReader(connection
					.getInputStream()));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (FeedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Get contents from links
		// Prepare to write Weka file
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(SAVE_FILE));
			bw.write("@relation webmd\n" + "@attribute title string\n"
					+ "@attribute link string\n"
					+ "@attribute content string\n" + "@data\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<SyndEntryImpl> entries = feed.getEntries();
		for (SyndEntryImpl entry : entries) {
			String outString = "\""
					+ entry.getTitle().replace('\"', ' ').replace('\'', ' ')
							.replace('\n', ' ') + "\",\"" + entry.getLink()
					+ "\",";
			Document doc = null;
			try {
				doc = Jsoup.connect(entry.getLink()).get();
			} catch (IOException e) {
				e.printStackTrace();
			}
			Element content = doc.getElementById("textArea");
			if (content != null)
				outString += "\""
						+ content.text().replace('\"', ' ').replace('\'', ' ')
								.replace('\n', ' ') + "\"\n";
			else
				outString += "\"\"\n";
			// write file
			try {
				System.out.println(outString);
				bw.write(outString);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
