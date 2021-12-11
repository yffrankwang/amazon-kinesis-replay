/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.amazonaws.samples.kinesis.replay.utils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.samples.kinesis.replay.events.JsonEvent;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

public class EventReader implements Iterator<JsonEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(EventReader.class);

	private final String bucketName;
	private final S3Client s3;
	private final Iterator<S3Object> s3Objects;

	private final JsonEvent.Parser eventParser;
	private String objectType;
	private CSVParser objectReader;
	private List<String> objectHeader;

	private JsonEvent next;

	public EventReader(S3Client s3, String bucketName, String prefix, float speedupFactor, String timestampAttributeName) {
		this.s3 = s3;
		this.bucketName = bucketName;
		this.eventParser = new JsonEvent.Parser(speedupFactor, timestampAttributeName);

		ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
		this.s3Objects = s3.listObjectsV2Paginator(request).contents().iterator();

		// initialize next and hasNext fields
		if (nextS3Object()) {
			nextRecord();
		}
	}

	public void seek(Instant timestamp) {
		while (next != null && next.timestamp.isBefore(timestamp)) {
			next();
		}
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	public void close() {
		if (objectReader != null) {
			try {
				objectReader.close();
			} catch (IOException e) {
				LOG.warn("failed to close object: {}", e);
			}
			
			objectReader = null;
		}
	}

	private boolean setObjectType(String key) {
		objectType = null;
		if (StringUtils.containsIgnoreCase(key, "yellow")) {
			objectType = "yellow";
			return true;
		}

		if (StringUtils.containsIgnoreCase(key, "green")) {
			objectType = "green";
			return true;
		}

		//TODO:
//		if (StringUtils.containsIgnoreCase(key, "fhvhv")) {
//			objectType = "fhvhv";
//			return true;
//		}
//		if (StringUtils.containsIgnoreCase(key, "fhv")) {
//			objectType = "fhv";
//			return true;
//		}
		return false;
	}

	private boolean nextS3Object() {
		while (s3Objects.hasNext()) {
			// if another object has been previously read, close it before opening another one
			close();
			
			// try to open the next S3 object
			S3Object s3Object = s3Objects.next();
	
			if (!StringUtils.endsWithIgnoreCase(s3Object.key(), ".csv") || !setObjectType(s3Object.key())) {
				LOG.info("skipping object s3://{}/{}", bucketName, s3Object.key());
				continue;
			}

			LOG.info("---------------------------------------------------");
			LOG.info("reading object s3://{}/{}", bucketName, s3Object.key());
			try {
				GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build();
				objectReader = CSVFormat.DEFAULT.parse(new InputStreamReader(s3.getObject(request)));
				
				Iterator<CSVRecord> objectIterator = objectReader.iterator();
				if (!objectIterator.hasNext()) {
					continue;
				}
				
				CSVRecord r = objectIterator.next();
				objectHeader = r.toList();
				if (objectHeader.isEmpty()) {
					LOG.warn("skipping object s3://{}/{} as it hash no csv header", bucketName, s3Object.key());
					continue;
				}
				
				DataNormalizer.normalizeHeader(objectHeader);
				
				return true;
			} catch (SdkClientException e) {
				// if we cannot read this object, skip it and try to read the next one
				LOG.warn("skipping object s3://{}/{} as it failed to open", bucketName, s3Object.key());
				LOG.debug("failed to open object", e);
				continue;
			} catch (IOException e) {
				LOG.warn("skipping object s3://{}/{} as it failed to read", bucketName, s3Object.key());
				LOG.debug("failed to read object", e);
				continue;
			}
		}

		return false;
	}
	
	private void nextRecord() {
		next = null;

		while (objectReader != null) {
			Iterator<CSVRecord> it = objectReader.iterator();
			while (it.hasNext()) {
				try {
					List<String> record = it.next().toList();
					Map<String, Object> data = DataNormalizer.list2map(objectHeader, record);
					data.put("type", objectType);
					next = eventParser.parse(data);
					if (next != null) {
						return;
					}
				} catch (Exception e) {
					Throwable t = e.getCause();
					if (t instanceof IOException) {
						LOG.warn("Failed to read record [{}]: {}", objectReader.getCurrentLineNumber(), t.getMessage());
						break;
					}
					LOG.warn("Failed to read record [{}]: {}", objectReader.getCurrentLineNumber(), e.getMessage());
				}
			}
			
			nextS3Object();
		}
	}

	@Override
	public JsonEvent next() {
		if (next == null) {
			return null;
		}

		JsonEvent result = next;
		nextRecord();
		return result;
	}
}
