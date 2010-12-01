/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package kafka.etl;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateUtils {

	// use PST timezone
	static DateTimeZone PST = DateTimeZone.forID("America/Los_Angeles");
	static DateTimeFormatter dateFormatter = DateUtils
			.getDateTimeFormatter("YYYYMMdd");
	static DateTimeFormatter hourFormatter = DateUtils
			.getDateTimeFormatter("YYYYMMddHH");
	static DateTimeFormatter minuteFormatter = DateUtils
			.getDateTimeFormatter("YYYYMMddHHmm");

	static DateTimeFormatter dateDirFormatter = DateUtils
			.getDateTimeFormatter("YYYYMMdd");
	static DateTimeFormatter hourDirFormatter = DateUtils
			.getDateTimeFormatter("YYYYMMdd" + Path.SEPARATOR + "HH");
	static DateTimeFormatter minuteDirFormatter = DateUtils
			.getDateTimeFormatter("YYYYMMdd" + Path.SEPARATOR + "HH"
					+ Path.SEPARATOR + "mm");

	private static final DateTimeFormatter displayFormatter = DateUtils
			.getDateTimeFormatter("YYYYMMdd-HH:mm:ss ZZ");

	public static String print(long timestamp) {
		return displayFormatter.print(timestamp);
	}

	public static enum TimeGranularity {
		DAY(1), HOUR(2), MINUTE(3);

		private int code;

		private TimeGranularity(int c) {
			code = c;
		}

		public int getCode() {
			return code;
		}

		public long getMilliSeconds() {
			switch (this) {
			case DAY:
				return 24 * 3600 * 1000;
			case HOUR:
				return 3600 * 1000;
			case MINUTE:
				return 60 * 1000;
			default:
				return 0;
			}
		}

		public static TimeGranularity getGranularity(String value) {
			for (TimeGranularity granularity : TimeGranularity.values()) {
				if (value.equalsIgnoreCase(granularity.name()))
					return granularity;
			}
			throw new RuntimeException("No partition granularity defined for "
					+ value);
		}
	}

	static public DateTimeFormatter getDateTimeFormatter(String str) {
		return DateTimeFormat.forPattern(str).withZone(PST);
	}

	public static String toPath(long timestamp, TimeGranularity granularity) {

		String filename = null;

		switch (granularity) {
		case DAY:
			filename = dateDirFormatter.print(timestamp); // dateTime.toString("yyyyMMdd")
															// ;
			break;
		case HOUR:
			filename = hourDirFormatter.print(timestamp); // dateTime.toString("yyyyMMdd_HH")
															// ;
			break;
		case MINUTE:
			filename = minuteDirFormatter.print(timestamp); // dateTime.toString("yyyyMMdd_HHmm");
			break;
		}
		return filename;
	}

	public static String toString(long timestamp, TimeGranularity granularity) {

		String filename = null;

		switch (granularity) {
		case DAY:
			filename = dateFormatter.print(timestamp); // dateTime.toString("yyyyMMdd")
														// ;
			break;
		case HOUR:
			filename = hourFormatter.print(timestamp); // dateTime.toString("yyyyMMdd_HH")
														// ;
			break;
		case MINUTE:
			filename = minuteFormatter.print(timestamp); // dateTime.toString("yyyyMMdd_HHmm");
			break;
		}
		return filename;
	}

	public static long toTimestamp(String timepart, TimeGranularity granularity)
			throws IOException {

		switch (granularity) {
		case DAY:
			if (timepart.length() < 8)
				throw new IOException("Invalid time format:" + timepart
						+ ". Should be yyyyMMdd[xxx]");
			return dateFormatter.parseMillis(timepart.substring(0, 8));
		case HOUR:
			if (timepart.length() < 10)
				throw new IOException("Invalid time format:" + timepart
						+ ". Should be yyyyMMddHH[xxx]");
			return hourFormatter.parseMillis(timepart.substring(0, 10));
		case MINUTE:
			if (timepart.length() < 12)
				throw new IOException("Invalid time format:" + timepart
						+ ". Should be yyyyMMddHHmm[xxx]");
			return minuteFormatter.parseMillis(timepart.substring(0, 12));
		default:
			throw new IOException("Undefined time granularity:" + granularity);
		}

	}

}
