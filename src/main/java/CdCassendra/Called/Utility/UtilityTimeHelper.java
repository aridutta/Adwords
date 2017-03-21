package CdCassendra.Called.Utility;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;

public class UtilityTimeHelper {
	Calendar today;

	public void method(TimerTask run) {
		today = Calendar.getInstance();
		today.set(Calendar.HOUR_OF_DAY, 6);
		today.set(Calendar.MINUTE, 0);
		today.set(Calendar.SECOND, 0);

		// every morning at 6am you run your task
		Timer timer = new Timer();
		timer.schedule(run, today.getTime(), 24 * 60 * 60 * 1000);
	}

	public String getYesterdayDateString() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		return dateFormat.format(cal.getTime());
	}

	public String getWeeklyDateString() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -7);
		return dateFormat.format(cal.getTime());
	}

	public String getMonthlyDateString() {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE , -28);
		return dateFormat.format(cal.getTime());
	}
	
}
