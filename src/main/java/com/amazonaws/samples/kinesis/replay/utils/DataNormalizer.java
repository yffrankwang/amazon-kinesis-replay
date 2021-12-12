package com.amazonaws.samples.kinesis.replay.utils;

import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataNormalizer {
	private static final FastDateFormat DATEFMT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

	private static final Logger LOG = LoggerFactory.getLogger(DataNormalizer.class);
	
	@SuppressWarnings("unchecked")
	private static Map<String, String> HEADERS = MapUtils.putAll(new HashMap<String, String>(), new String[][] {
		{ "ratecodeid", "rate_code" },
		{ "vendorid", "vendor_id" },
		{ "dolocationid", "dropoff_location_id" },
		{ "pulocationid", "pickup_location_id" },
		{ "tpep_dropoff_datetime", "dropoff_datetime" },
		{ "tpep_pickup_datetime", "pickup_datetime" },
		{ "lpep_dropoff_datetime", "dropoff_datetime" },
		{ "lpep_pickup_datetime", "pickup_datetime" },
	});

	private static final Set<String> REQUIRED = new HashSet<String>();
	private static final Set<String> DATETIMES = new HashSet<String>();
	private static final Set<String> DOUBLES = new HashSet<String>();
	private static final Set<String> LONGS = new HashSet<String>();

	static {
		CollectionUtils.addAll(REQUIRED, new Object[] {"dropoff_datetime", "pickup_datetime" });
		CollectionUtils.addAll(DATETIMES, new Object[] {"dropoff_datetime", "pickup_datetime" });
		CollectionUtils.addAll(DOUBLES, new Object[] {
			"dropoff_latitude", 
			"dropoff_longitude", 
			"pickup_latitude", 
			"pickup_longitude"
		});
		CollectionUtils.addAll(LONGS, new Object[] { "passenger_count" });
	}

	public static Instant parseInstant(String s) {
		try {
			return Instant.ofEpochMilli(DATEFMT.parse(s).getTime());
		} catch (ParseException e) {
			return Instant.EPOCH;
		}
	}

	public static List<String> normalizeHeader(List<String> header) {
		for (int i = 0; i < header.size(); i++) {
			header.set(i,normalizeHeader(header.get(i)));
		}
		return header;
	}
	
	public static String normalizeHeader(String name) {
		name = StringUtils.strip(name);
		name = StringUtils.lowerCase(name);
		String v = HEADERS.get(name);
		return v == null ? name : v;
	}

	public static Map<String, Object> normalizeRecord(Map<String, Object> record) {
		for (String k : REQUIRED) {
			String v = (String)record.get(k);
			if (StringUtils.isEmpty(v)) {
				LOG.warn("Discard invalid record " + k + "=" + v + " " + record);
				return null;
			}
		}
		
		for (Entry<String, Object> en : record.entrySet()) {
			String k = en.getKey();
			String v = (String)en.getValue();
			try {
				if (DATETIMES.contains(k)) {
					DATEFMT.parse(v);
				} else if ("trip_distance".equals(k)) {
					if (StringUtils.isEmpty(v)) {
						en.setValue(0L);
					} else {
						en.setValue(mile2meter(Double.parseDouble(v)));
					}
				} else if (DOUBLES.contains(k)) {
					if (StringUtils.isEmpty(v)) {
						en.setValue((double)0);
					} else {
						en.setValue(Double.parseDouble(v));
					}
				} else if (LONGS.contains(k)) {
					if (StringUtils.isEmpty(v)) {
						en.setValue((long)0);
					} else {
						en.setValue(Long.parseLong(v));
					}
				}
			} catch (Exception e) {
				LOG.warn("Discard invalid record " + k + "=" + v + " " + record);
				return null;
			}
		}
		return record;
	}

	public static Map<String, Object> list2map(List<String> header, List<String> record) {
		Map<String, Object> data = new TreeMap<String, Object>();
		
		for (int i = 0; i < header.size(); i++) {
			String h = header.get(i);
			String v = i < record.size() ? record.get(i) : "";
			data.put(h, v);
		}
		
		return data;
	}

	public static long mile2meter(double mile) {
		return (long)(mile * 1609.34);
	}
}
