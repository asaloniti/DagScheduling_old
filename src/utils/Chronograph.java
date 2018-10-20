package utils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class Chronograph {

	public static String getTimeAsString(int time_in_secs) {
		int h = time_in_secs / 3600;
		time_in_secs = time_in_secs % 3600;
		int m = time_in_secs / 60;
		int s = time_in_secs % 60;
		return String.format("%02d:%02d:%02d", h, m, s);
	}

	private List<Long> laps;

	public Chronograph() {
		laps = new ArrayList<Long>();
		reset();
	}

	public void reset() {
		laps.clear();
		lap();
	}

	public void lap() {
		// laps.add(System.currentTimeMillis());
		laps.add(Calendar.getInstance().getTimeInMillis());
	}

	public long getElapsedTimeInMilliseconds() {
		return laps.get(laps.size() - 1) - laps.get(0);
	}

	public int getElapsedTimeInSeconds() {
		return (int) (getElapsedTimeInMilliseconds() / 1000l);
	}

	public double getElapsedTimeInSecondsAsDouble() {
		return getElapsedTimeInMilliseconds() / 1000.;
	}

	public String getElapsedTimeAsString() {
		return getTimeAsString(getElapsedTimeInSeconds());
	}

}
