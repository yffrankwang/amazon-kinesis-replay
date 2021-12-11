package com.amazonaws.samples.kinesis.replay.utils;

import java.time.Instant;
import java.util.TimeZone;

import org.apache.commons.lang3.time.FastDateFormat;
import org.junit.Test;

public class DateParseTest {
	
	@Test
	public void testDateParse() throws Exception {
		FastDateFormat nycFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("America/New_York"));
		FastDateFormat localFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
		FastDateFormat isoFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSZ", TimeZone.getTimeZone("GMT"));
		FastDateFormat isoFormat2 = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSZ", TimeZone.getTimeZone("GMT"));

		String s = "2020-01-01 00:00:00";
		System.out.println(nycFormat.parse(s));
		System.out.println(nycFormat.parse(s).getTime());
		System.out.println(localFormat.parse(s));
		System.out.println(localFormat.parse(s).getTime());
		
		System.out.println(isoFormat.format(nycFormat.parse(s)));
		System.out.println(isoFormat.format(localFormat.parse(s)));
		System.out.println(isoFormat2.format(localFormat.parse(s)));

		s = "2020-01-01T00:00:00Z";
		System.out.println(Instant.parse(s));
	}
}
