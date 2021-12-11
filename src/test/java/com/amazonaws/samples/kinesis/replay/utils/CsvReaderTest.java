package com.amazonaws.samples.kinesis.replay.utils;

import java.io.FileReader;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CsvReaderTest {
	public static void main(String[] args) throws Exception {
		FileReader fr = new FileReader("D:\\Develop\\Projects\\aws\\tlc\\data\\green_tripdata_2021-01.csv");
		CSVParser csv = CSVFormat.DEFAULT.parse(fr);

		Iterator<CSVRecord> it = csv.iterator();
		while (it.hasNext()) {
			System.out.println(it.next());
		}
		
		csv.close();
	}
}
