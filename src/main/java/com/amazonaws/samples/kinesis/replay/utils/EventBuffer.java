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

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.samples.kinesis.replay.events.JsonEvent;

public class EventBuffer extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(EventBuffer.class);

	private boolean hasNext = true;

	private EventReader reader;
	private final int bufferSize;
	private final Semaphore semaphore;
	private final PriorityBlockingQueue<JsonEvent> eventPool;

	public EventBuffer(EventReader reader, int bufferSize) {
		this.reader = reader;
		this.bufferSize = bufferSize;
		this.semaphore = new Semaphore(bufferSize);
		this.eventPool = new PriorityBlockingQueue<>(bufferSize, JsonEvent.timestampComparator);
	}

	public void run() {
		LOG.info("Starting event buffer");

		while (reader.hasNext()) {
			try {
				LOG.debug("require a semaphore: {} / {}", semaphore.availablePermits(), eventPool.size());
				semaphore.acquire();
				
				JsonEvent je = reader.next();

				LOG.debug("add event: {}", je.timestamp);
				eventPool.add(je);
				LOG.debug("event added: {}", eventPool.size());
			} catch (InterruptedException e) {
				LOG.debug("interrupted");
			}
		}

		hasNext = false;
		LOG.info("Event buffer thread exit.");
	}

	public boolean hasNext() {
		return hasNext || !eventPool.isEmpty();
	}

	public JsonEvent take() throws InterruptedException {
		LOG.debug("take a event of {} / {}", semaphore.availablePermits(), eventPool.size());
		JsonEvent je = eventPool.take();

		LOG.debug("release semaphore {}", semaphore.availablePermits());
		semaphore.release();
		
		return je;
	}

	public JsonEvent peek() {
		return eventPool.peek();
	}

	public int size() {
		return eventPool.size();
	}

	public void fill() throws InterruptedException {
		while (eventPool.size() < bufferSize) {
			Thread.sleep(100);
		}
	}

}
