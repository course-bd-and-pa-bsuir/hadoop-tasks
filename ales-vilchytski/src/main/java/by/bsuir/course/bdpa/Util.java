package by.bsuir.course.bdpa;

import java.util.Calendar;
import java.util.TimeZone;

public class Util {

	public static String timestamp() {
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		return String.format("%4d-%02d-%02d.%02d-%02d-%02d", 
				c.get(Calendar.YEAR),
				c.get(Calendar.MONTH),
				c.get(Calendar.DAY_OF_MONTH),
				c.get(Calendar.HOUR_OF_DAY), 
				c.get(Calendar.MINUTE),
				c.get(Calendar.SECOND));
	}
	
}
