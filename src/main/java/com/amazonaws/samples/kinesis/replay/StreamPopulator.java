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

package com.amazonaws.samples.kinesis.replay;

import com.amazonaws.regions.Regions;
import com.amazonaws.samples.kinesis.replay.events.JsonEvent;
import com.amazonaws.samples.kinesis.replay.utils.BackpressureSemaphore;
import com.amazonaws.samples.kinesis.replay.utils.EventBuffer;
import com.amazonaws.samples.kinesis.replay.utils.EventReader;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;


public class StreamPopulator {
	private static final Logger LOG = LoggerFactory.getLogger(StreamPopulator.class);

	private static final String DEFAULT_REGION_NAME = Regions.getCurrentRegion()==null ? "us-east-1" : Regions.getCurrentRegion().getName();


	private final String streamName;
	private final String bucketName;
	private final String objectPrefix;
	private final long statisticsFrequencyMillies;
	private final KinesisProducer kinesisProducer;
	private final EventBuffer eventBuffer;
	private final BackpressureSemaphore<UserRecordResult> backpressureSemaphore;
	private final boolean notSend;

	public StreamPopulator(String bucketRegion,
			String bucketName,
			String objectPrefix,
			String streamRegion,
			String streamName,
			boolean aggregate,
			String timestampAttributeName,
			float speedupFactor,
			long statisticsFrequencyMillies,
			Instant seekToEpoch,
			int bufferSize,
			int maxOutstandingRecords,
			boolean noBackpressure,
			boolean notSend) {

		KinesisProducerConfiguration producerConfiguration = new KinesisProducerConfiguration()
				.setRegion(streamRegion)
				.setRecordTtl(60_000)
				.setThreadingModel(KinesisProducerConfiguration.ThreadingModel.POOLED)
				.setAggregationEnabled(aggregate);

		final S3Client s3 = S3Client.builder().region(Region.of(bucketRegion)).build();

		this.notSend = notSend;
		this.streamName = streamName;
		this.bucketName = bucketName;
		this.objectPrefix = objectPrefix;
		this.statisticsFrequencyMillies = statisticsFrequencyMillies;
		this.kinesisProducer = new KinesisProducer(producerConfiguration);

		EventReader eventReader = new EventReader(s3, bucketName, objectPrefix, speedupFactor, timestampAttributeName);
		if (seekToEpoch != null) {
			eventReader.seek(seekToEpoch);
		}

		eventBuffer = new EventBuffer(eventReader, bufferSize);
		eventBuffer.start();

		if (!noBackpressure) {
			this.backpressureSemaphore = new BackpressureSemaphore<>(maxOutstandingRecords);
		} else {
			this.backpressureSemaphore = null;
		}
	}


	private void populate() {
		long statisticsBatchEventCount = 0;
		long statisticsLastOutputTimeslot = 0;

		try {
			LOG.info("populating internal event buffer");

			eventBuffer.fill();

			JsonEvent event = eventBuffer.peek();

			if (event == null) {
				LOG.error("didn't find any events to replay in s3://{}/{}", bucketName, objectPrefix);

				return;
			}

			final long ingestionStartTime = System.currentTimeMillis();

			LOG.info("starting to ingest events into stream {}", streamName);

			do {
				event = eventBuffer.take();

				Duration replayTimeGap = Duration.between(event.ingestionTime, Instant.now());

				if (replayTimeGap.isNegative()) {
					LOG.trace("sleep {} ms", replayTimeGap);
					Thread.sleep(-replayTimeGap.toMillis());
				}

				ingestEvent(event);

				statisticsBatchEventCount++;

				//output statistics every statisticsFrequencyMillies ms
				if ((System.currentTimeMillis() - ingestionStartTime) / statisticsFrequencyMillies != statisticsLastOutputTimeslot) {
					double statisticsBatchEventRate = Math.round(1000.0 * statisticsBatchEventCount / statisticsFrequencyMillies);

					Instant dropoffTime = eventBuffer.peek().timestamp;

					LOG.info("all events with dropoff time until {} have been sent ({} events/sec, {} buffer size)",
						dropoffTime, statisticsBatchEventRate, eventBuffer.size());

					statisticsBatchEventCount = 0;
					statisticsLastOutputTimeslot = (System.currentTimeMillis() - ingestionStartTime) / statisticsFrequencyMillies;
				}
			} while (eventBuffer.hasNext());

			LOG.info("all events have been sent");
		} catch (InterruptedException e) {
			//allow thread to exit
		} finally {
			eventBuffer.interrupt();

			kinesisProducer.flushSync();
			kinesisProducer.destroy();
		}
	}

	private JsonEvent traceEvent;
	private void ingestEvent(JsonEvent event) {
		if (notSend) {
			if (traceEvent == null || Duration.between(traceEvent.ingestionTime, event.ingestionTime).toMillis() > 1000) {
				traceEvent = event;
				LOG.info("Event: {}", event.payload);
			}
			return;
		}
		
		//queue the next event for ingestion to the Kinesis stream through the KPL
		ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(
				streamName, Integer.toString(event.hashCode()), event.toByteBuffer());

		if (backpressureSemaphore != null) {
			//block if too many events are buffered locally
			backpressureSemaphore.acquire(f);
		}

		LOG.trace("sent event {}", event);
	}

	public static void main(String[] args) throws ParseException {
		Options options = new Options()
				.addOption("bucketRegion", true, "the region of the S3 bucket")
				.addOption("bucketName", true, "the bucket containing the raw event data")
				.addOption("objectPrefix", true, "the prefix of the objects containing the raw event data")
				.addOption("streamRegion", true, "the region of the Kinesis stream")
				.addOption("streamName", true, "the name of the kinesis stream the events are sent to")
				.addOption("speedup", true, "the speedup factor for replaying events into the kinesis stream")
				.addOption("timestampAttributeName", true, "the name of the attribute that contains the timestamp according to which events ingested into the stream")
				.addOption("aggregate", "turn on aggregation of multiple events into a single Kinesis record")
				.addOption("seek", true, "start replaying events at given timestamp")
				.addOption("statisticsFrequency", true, "print statistics every statisticFrequency ms")
				.addOption("bufferSize", true, "size of the buffer that holds events to sent to the stream")
				.addOption("maxOutstandingRecords", true, "block producer if more than maxOutstandingRecords are in flight")
				.addOption("noBackpressure", "don't block producer if too many messages are in flight")
				.addOption("notSend", "do not send to kinesis")
				.addOption("help", "print this help message");

		CommandLine line = new DefaultParser().parse(options, args);

		if (line.hasOption("help")) {
			new HelpFormatter().printHelp(MethodHandles.lookup().lookupClass().getName(), options);
			return;
		}

		Instant seekToEpoch = null;
		if (line.hasOption("seek")) {
			seekToEpoch = Instant.parse(line.getOptionValue("seek"));
		}

		StreamPopulator populator = new StreamPopulator(line.getOptionValue("bucketRegion", "us-east-1"),
			line.getOptionValue("bucketName", "nyc-tlc"),
			line.getOptionValue("objectPrefix", "trip data/"),
			line.getOptionValue("streamRegion", DEFAULT_REGION_NAME),
			line.getOptionValue("streamName", "taxi-trip-events"),
			line.hasOption("aggregate"),
			line.getOptionValue("timestampAttributeName", "dropoff_datetime"),
			Float.parseFloat(line.getOptionValue("speedup", "3600")),
			Long.parseLong(line.getOptionValue("statisticsFrequency", "20000")),
			seekToEpoch,
			Integer.parseInt(line.getOptionValue("bufferSize", "100000")),
			Integer.parseInt(line.getOptionValue("maxOutstandingRecords", "10000")),
			line.hasOption("noBackpressure"),
			line.hasOption("notSend")
			);

		populator.populate();
	}
}
