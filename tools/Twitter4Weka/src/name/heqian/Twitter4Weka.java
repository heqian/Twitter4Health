/**
 * 
 */
package name.heqian;

import java.util.Timer;

/**
 * @author heqian
 * 
 */
public class Twitter4Weka {
	public static void main(String[] args) {
		Timer timer = new Timer();
		SearchTask task = new SearchTask();
		int period = 60;

		switch (args.length) {
		case 0:
			System.out.println("Usage: Twitter4Weka.jar [query] [seconds]");
			break;
		case 2:
			period = Integer.parseInt(args[1]);
		case 1:
			task.setQuery(args[0]);
			break;
		default:
			break;
		}
		timer.scheduleAtFixedRate(task, 0, period * 1000);
	}
}
