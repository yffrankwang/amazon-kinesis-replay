package com.amazonaws.samples.kinesis.replay.utils;

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

public class DataNormalizer {
	public static final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

	@SuppressWarnings("unchecked")
	private static Map<String, String> headers = MapUtils.putAll(new HashMap<String, String>(), new String[][] {
		{ "ratecodeid", "rate_code" },
		{ "vendorid", "vendor_id" },
		{ "dolocationid", "dropoff_location_id" },
		{ "pulocationid", "pickup_location_id" },
		{ "tpep_dropoff_datetime", "dropoff_datetime" },
		{ "tpep_pickup_datetime", "pickup_datetime" },
		{ "lpep_dropoff_datetime", "dropoff_datetime" },
		{ "lpep_pickup_datetime", "pickup_datetime" },
	});

	private static final Set<String> datetimes = new HashSet<String>();
	private static final Set<String> doubles = new HashSet<String>();
	private static final Set<String> longs = new HashSet<String>();
	static {
		CollectionUtils.addAll(datetimes, new Object[] {"dropoff_datetime", "pickup_datetime" });
		CollectionUtils.addAll(doubles, new Object[] {
			"dropoff_latitude", 
			"pickup_longitude", 
			"dropoff_latitude", 
			"pickup_longitude"
		});
		CollectionUtils.addAll(longs, new Object[] { "passenger_count" });
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
		String v = headers.get(name);
		return v == null ? name : v;
	}

	public static void normalizeRecord(Map<String, Object> record) {
		for (Entry<String, Object> en : record.entrySet()) {
			String k = en.getKey();
			String v = (String)en.getValue();
			try {
				if (datetimes.contains(k)) {
					dateFormat.parse(v);
				} else if ("trip_distance".equals(k)) {
					if (StringUtils.isEmpty(v)) {
						en.setValue(0L);
					} else {
						en.setValue(mile2meter(Double.parseDouble(v)));
					}
				} else if (doubles.contains(k)) {
					if (StringUtils.isEmpty(v)) {
						en.setValue((double)0);
					} else {
						en.setValue(Double.parseDouble(v));
					}
				} else if (longs.contains(k)) {
					if (StringUtils.isEmpty(v)) {
						en.setValue((long)0);
					} else {
						en.setValue(Long.parseLong(v));
					}
				}
			} catch (Exception e) {
				throw new IllegalArgumentException("Invalid value " + k + ": " + v);
			}
		}
	}

	public static Map<String, Object> list2map(List<String> header, List<String> record) {
		Map<String, Object> data = new TreeMap<String, Object>();
		
		for (int i = 0; i < headers.size(); i++) {
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
