package com.amazonaws.samples.kinesis.replay.utils;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import com.amazonaws.samples.kinesis.replay.events.JsonEvent;

public class DateConvertTest {
	private void convert(String fin) throws Exception {
		if (!new File(fin).canRead()) {
			return;
		}
		
		JsonEvent.Parser jp = new JsonEvent.Parser(3600, "dropoff_datetime");

		FileReader fr = new FileReader(fin);
		FileWriter fw = new FileWriter(fin + ".json");

		CSVParser objectReader = CSVFormat.DEFAULT.parse(fr);
		
		Iterator<CSVRecord> it = objectReader.iterator();

		CSVRecord r = it.next();
		List<String> objectHeader = r.toList();
		
		DataNormalizer.normalizeHeader(objectHeader);

		while (it.hasNext()) {
			List<String> record = it.next().toList();
			Map<String, Object> data = DataNormalizer.list2map(objectHeader, record);
			data.put("type", "green");
			JsonEvent next = jp.parse(data);
			if (next == null) {
				return;
			}
			fw.write(next.toString());
		}
		
		fr.close();
		fw.close();
	}
	
	@Test
	public void testDateConvertGreen() throws Exception {
		convert("D:\\Develop\\Projects\\aws\\tlc\\data\\green_tripdata_2018-01.csv");
		convert("D:\\Develop\\Projects\\aws\\tlc\\data\\green_tripdata_2014-01.csv1");
	}
	
	@Test
	public void testDateConvertYellow() throws Exception {
		convert("D:\\Develop\\Projects\\aws\\tlc\\data\\yellow_tripdata_2018-01.csv");
	}
}
