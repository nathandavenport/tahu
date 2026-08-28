/*
 * Licensed Materials - Property of Cirrus Link Solutions
 * Copyright (c) 2026 Cirrus Link Solutions LLC - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */
package org.eclipse.tahu.mqtt.test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttWireMessage;
import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.mqtt.ClientCallback;
import org.eclipse.tahu.mqtt.MqttClientId;
import org.eclipse.tahu.mqtt.MqttServerName;
import org.eclipse.tahu.mqtt.MqttServerUrl;
import org.eclipse.tahu.mqtt.TahuClient;
import org.eclipse.tahu.mqtt.TahuMqttAsyncClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for the FIFO publish buffer added to {@link TahuClient}.
 *
 * IMM-5395 - MQTT Transmission UNS Transmitter Thread Explosion / OOM.
 *
 * These exercise the class against a fake Paho client rather than a broker, so the in-flight window can be held at zero
 * deterministically - which is the state that used to deadlock the client permanently.
 */
public class TahuClientPublishBufferTest {

	private static final long TIMEOUT_MS = 5000;
	private static final long DRAIN_POLL_MS = 250;

	private TahuClient tahuClient;
	private FakeMqttClient fakeClient;

	@BeforeMethod
	public void setUp() throws Exception {
		tahuClient = newTahuClient();
		fakeClient = new FakeMqttClient();
	}

	@AfterMethod
	public void tearDown() throws Exception {
		if (tahuClient != null) {
			/*
			 * Bounded, because the shutdown needs publishOrderLock and a deadlock regression is exactly the state
			 * where nobody can have it. Left unbounded, a client wedged the way IMM-5395 wedged it would hang the
			 * JVM here and the run would die by CI timeout with no failing test named. This turns that into a
			 * teardown failure that says which test wedged the client.
			 */
			Thread shutdown = new Thread(() -> {
				try {
					invoke(tahuClient, "shutdownPublishBufferDrainThread");
				} catch (Exception e) {
					// Reported by the join below if it matters
				}
			}, "test-teardown");
			shutdown.setDaemon(true);
			shutdown.start();
			shutdown.join(2000);
			Assert.assertFalse(shutdown.isAlive(),
					"The client could not be shut down - something is holding publishOrderLock, which is what a "
							+ "deadlock regression looks like from here");
		}
		if (fakeClient != null) {
			fakeClient.shutdownAckThread();
		}
	}

	// ------------------------------------------------------------------------------------------------------------
	// The deadlock itself
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * The regression test for IMM-5395.
	 *
	 * With the window exhausted, publish() must return promptly instead of parking while holding messageLock. If this
	 * hangs, the deadlock is back.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void publishDoesNotBlockWhenNoPermitsAvailable() throws Exception {
		wire(0, 8);

		long start = System.currentTimeMillis();
		IMqttDeliveryToken token = tahuClient.publish("topic/a", "a".getBytes(), 1, false);
		long elapsed = System.currentTimeMillis() - start;

		Assert.assertNull(token, "A buffered publish must return null, not a token");
		Assert.assertTrue(elapsed < 1000, "publish() blocked for " + elapsed + "ms - it must never wait for a permit");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1);
		Assert.assertEquals(fakeClient.publishedTopics().size(), 0, "Nothing should have reached the MQTT client");
	}

	/**
	 * deliveryComplete() must be able to return a permit while a publisher is in flight. Under the old code the
	 * publisher held messageLock while parked in acquire(), so this callback could never run.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void deliveryCompleteCanReturnPermitsWhilePublishing() throws Exception {
		wire(1, 8);

		IMqttDeliveryToken token = tahuClient.publish("topic/a", "a".getBytes(), 1, false);
		Assert.assertNotNull(token, "First publish should go inline while a permit is free");
		Assert.assertEquals(availablePermits(), 0, "Precondition: the in-flight window is now exhausted");

		/*
		 * A second publisher meeting the exhausted window. This is the state that deadlocked: the old code took
		 * messageLock and then blocked in semaphore.acquire() inside it, so the publisher held the monitor
		 * deliveryComplete() needs to hand a permit back, and neither could proceed. It must now return instead -
		 * buffered, with a null token - without holding anything deliveryComplete() needs.
		 */
		final CountDownLatch publisherEntered = new CountDownLatch(1);
		final CountDownLatch publisherReturned = new CountDownLatch(1);
		final AtomicReference<Object> publisherResult = new AtomicReference<>();
		Thread publisherThread = new Thread(() -> {
			publisherEntered.countDown();
			try {
				publisherResult.set(tahuClient.publish("topic/blocked", "b".getBytes(), 1, false));
			} catch (Throwable t) {
				publisherResult.set(t);
			}
			publisherReturned.countDown();
		}, "fake-publisher");
		publisherThread.setDaemon(true);
		publisherThread.start();
		Assert.assertTrue(publisherEntered.await(2, TimeUnit.SECONDS), "Publisher thread never started");

		// The Paho callback thread returning the permit - this is the call that used to block forever
		final CountDownLatch done = new CountDownLatch(1);
		Thread callbackThread = new Thread(() -> {
			tahuClient.deliveryComplete(token);
			done.countDown();
		}, "fake-MQTT-Call");
		callbackThread.setDaemon(true);
		callbackThread.start();

		Assert.assertTrue(done.await(2, TimeUnit.SECONDS),
				"deliveryComplete() blocked while a publisher met the exhausted window - the deadlock has returned");
		Assert.assertTrue(publisherReturned.await(2, TimeUnit.SECONDS),
				"The publisher parked inside the publish path instead of buffering - it is holding the monitor "
						+ "deliveryComplete() needs");
		Assert.assertNull(publisherResult.get(),
				"A publisher with no permit must buffer and return null, not block and not throw: "
						+ publisherResult.get());

		// The permit does not come to rest as a free permit: the drain takes it straight back for the message that
		// was waiting on it. Asserting that end state proves the whole chain, rather than just that nothing hung.
		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/a", "topic/blocked"),
				"The permit deliveryComplete() returned must have carried the buffered message out");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Ordering
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * QoS 0 is never buffered. It takes no permit and is not subject to backpressure, so it goes to Paho immediately
	 * even while QoS > 0 messages are queued - which means it can overtake them. That is the intended trade-off:
	 * MQTT only orders within a QoS level, and holding live fire-and-forget data behind a stalled acknowledged queue
	 * would make it stale for no delivery benefit.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void qos0IsSentImmediatelyEvenWhenBufferIsNonEmpty() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/qos1", "1".getBytes(), 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "QoS 1 with no permit must buffer");

		IMqttDeliveryToken token = tahuClient.publish("topic/qos0", "0".getBytes(), 0, false);

		Assert.assertNotNull(token, "An immediately published QoS 0 message must return a token, not null");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "QoS 0 must not be added to the buffer");
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/qos0"),
				"QoS 0 must reach Paho immediately, ahead of the queued QoS 1");
	}

	/** QoS 0 must stay immediate even when the buffer is at capacity - it is not subject to the capacity limit. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void qos0IsSentImmediatelyWhenBufferIsFull() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferCapacity(2);

		tahuClient.publish("topic/a", "x".getBytes(), 1, false);
		tahuClient.publish("topic/b", "x".getBytes(), 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 2, "Buffer should be at capacity");

		tahuClient.publish("topic/qos0", "0".getBytes(), 0, false);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 2);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/qos0"),
				"A full buffer must not block QoS 0");
	}

	/** With an empty buffer there is nothing to overtake, so QoS 0 goes straight out. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void qos0PublishesInlineWhenBufferIsEmpty() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/qos0", "0".getBytes(), 0, false);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/qos0"));
	}

	/**
	 * Buffered QoS > 0 messages come out of the drain in publish order while every send succeeds.
	 *
	 * This asserts the happy path only, and deliberately so: FIFO here is best effort, not a guarantee. It is not held
	 * across a failed send - publishOrderLock is released between the failure and the re-queue, so a concurrent
	 * publisher can go inline ahead of the message going back to the head. That gap is accepted rather than fixed
	 * (MQTT does not order across an in-flight window greater than one either, and Sparkplug sequence-bearing traffic
	 * publishes at QoS 0 and never reaches this buffer), so do not tighten this test into a guarantee the class does
	 * not make.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void drainPreservesPublishOrderOfBufferedMessages() throws Exception {
		wire(0, 8);

		List<String> expected = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			String topic = "topic/" + i;
			tahuClient.publish(topic, "x".getBytes(), 1, false);
			expected.add(topic);
		}
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 8);

		startDrain();
		releasePermits(8);

		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics(), expected, "Drain must preserve FIFO order");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Buffer contents and head-of-line behaviour
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * The buffer must only ever hold QoS > 0. This is the invariant the drain depends on: it takes a permit before
	 * dequeuing, so a buffered QoS 0 message - which needs no permit and is never ACKed - would stall the buffer for
	 * as long as the in-flight window stayed exhausted.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferNeverHoldsQos0Messages() throws Exception {
		wire(0, 8);

		for (int i = 0; i < 10; i++) {
			tahuClient.publish("topic/qos1-" + i, "x".getBytes(), 1, false);
			tahuClient.publish("topic/qos0-" + i, "x".getBytes(), 0, false);
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 10, "Only the QoS 1 messages should be buffered");
		Assert.assertEquals(fakeClient.publishedTopics().size(), 10, "Every QoS 0 message should have gone out");
		for (String topic : fakeClient.publishedTopics()) {
			Assert.assertTrue(topic.startsWith("topic/qos0-"), "Only QoS 0 should have been published: " + topic);
		}

		// QoS 0 never consumes a permit
		Assert.assertEquals(availablePermits(), 0);
	}

	/** A QoS 1 head with no permits must stay put rather than being dropped or reordered. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void qos1HeadWaitsForAPermit() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/first", "x".getBytes(), 1, false);
		tahuClient.publish("topic/second", "y".getBytes(), 1, false);

		startDrain();
		Thread.sleep(DRAIN_POLL_MS * 3);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 2, "Nothing should drain with no permits available");
		Assert.assertEquals(fakeClient.publishedTopics().size(), 0);

		releasePermits(2);
		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/first", "topic/second"));
	}

	// ------------------------------------------------------------------------------------------------------------
	// Capacity
	// ------------------------------------------------------------------------------------------------------------

	/** At capacity the newest publish is rejected so the caller can fall back to store-and-forward. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferRejectsWhenFull() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferCapacity(3);

		for (int i = 0; i < 3; i++) {
			tahuClient.publish("topic/" + i, "x".getBytes(), 1, false);
		}
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 3);

		try {
			tahuClient.publish("topic/overflow", "x".getBytes(), 1, false);
			Assert.fail("Publishing past capacity must throw so the caller can store the message");
		} catch (TahuException e) {
			Assert.assertTrue(e.getMessage().contains("buffer is full"), "Unexpected message: " + e.getMessage());
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 3, "A rejected publish must not be buffered");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 1,
				"A capacity rejection must be counted so the loss is visible to an operator");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 0,
				"A refusal the caller was told about is not a discard - publish() threw, so nothing was lost here");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Failed sends
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * An inline send failure has no buffer to fall back on, so it must surface to the caller rather than being
	 * swallowed - the caller is the only layer that can decide to store or resend it.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void inlineSendFailureReachesCallerAndReturnsPermit() throws Exception {
		wire(4, 8);

		fakeClient.failNextPublishes(1);
		try {
			tahuClient.publish("topic/first", "x".getBytes(), 1, false);
			Assert.fail("An inline send failure must reach the caller");
		} catch (TahuException expected) {
			// expected
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0, "An inline failure must not silently buffer");
		Assert.assertEquals(availablePermits(), 4, "A failed inline send must return its permit");
	}

	/**
	 * A buffered message that keeps failing is retried across a window of wall clock, then dropped and counted.
	 *
	 * The budget is time, not attempts, and that distinction is the whole point. Nothing on the failure path delays -
	 * the buffer is non-empty so the drain's wait() is skipped, the client still reports connected so its poll sleep
	 * is skipped, and tryAcquire() returns at once because the failed send released its permit - so a pure attempt
	 * count was consumed in about two milliseconds and gave a transient failure no chance to clear.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 3)
	public void bufferedSendFailureIsRetriedAcrossTheWindowThenDroppedAndCounted() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/poison", "x".getBytes(), 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1);

		fakeClient.failNextPublishes(Integer.MAX_VALUE);
		startDrain();
		releasePermits(8);

		long start = System.currentTimeMillis();
		awaitBufferDepth(0, TIMEOUT_MS * 2);
		long elapsed = System.currentTimeMillis() - start;

		Assert.assertTrue(elapsed >= 500,
				"The retry budget must span wall clock, not iterations - dropped after only " + elapsed + "ms");
		Assert.assertTrue(fakeClient.publishAttempts() >= 3,
				"The window should still afford about three attempts, made " + fakeClient.publishAttempts());
		Assert.assertEquals(availablePermits(), 8, "Every permit taken for a failed send must be returned");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 1,
				"A dropped message is lost data and must be counted - publish() told the caller it was queued");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 0,
				"A message this client accepted and then lost is a discard, not a capacity rejection");
	}

	/**
	 * A message that fails once then succeeds must still be delivered, ahead of what is still queued behind it.
	 *
	 * Single-threaded on purpose. It proves the re-queue goes to the head rather than the tail; it does NOT prove the
	 * message keeps its place against a concurrent publisher, which it would not - see
	 * {@link #drainPreservesPublishOrderOfBufferedMessages()} for why that gap is accepted.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void requeuedMessageKeepsItsPlaceInLine() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/first", "x".getBytes(), 1, false);
		tahuClient.publish("topic/second", "x".getBytes(), 1, false);

		fakeClient.failNextPublishes(1);
		startDrain();
		releasePermits(8);

		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of("topic/first", "topic/second"),
				"A re-queued message must go back to the head, not the tail");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Permit accounting
	// ------------------------------------------------------------------------------------------------------------

	/** The pre-existing leak: a publish() that threw left its permit unreturned because token was null. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void failedPublishDoesNotLeakAPermit() throws Exception {
		wire(8, 8);

		for (int i = 0; i < 5; i++) {
			fakeClient.failNextPublishes(1);
			try {
				tahuClient.publish("topic/" + i, "x".getBytes(), 1, false);
			} catch (TahuException expected) {
				// expected
			}
		}

		Assert.assertEquals(availablePermits(), 8, "Permits must be returned when the send never happened");
	}

	/** A successful publish hands its permit to deliveryComplete(), which returns it on the ACK. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void permitsBalanceAcrossAFullPublishAckCycle() throws Exception {
		wire(8, 8);

		List<IMqttDeliveryToken> tokens = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			tokens.add(tahuClient.publish("topic/" + i, "x".getBytes(), 1, false));
		}
		Assert.assertEquals(availablePermits(), 0);

		for (IMqttDeliveryToken token : tokens) {
			tahuClient.deliveryComplete(token);
		}

		Assert.assertEquals(availablePermits(), 8, "Permits must return to full after every message is ACKed");
	}

	// ------------------------------------------------------------------------------------------------------------
	// End-to-end stall and recovery
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * The full backpressure cycle with permits returned the way the real system returns them - through
	 * deliveryComplete() on a separate callback thread - rather than by releasing the semaphore directly.
	 *
	 * This is the closest unit-level analogue of the incident: publish far past the in-flight window, let the buffer
	 * absorb the excess, and let ACKs alone drive it back down. It also exercises the lock ordering under contention,
	 * because the ACK thread must take messageLock while a publisher may be holding publishOrderLock -> messageLock.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 6)
	public void bufferDrainsAsAcksReturnPermits() throws Exception {
		final int window = 4;
		final int total = 40;
		wire(window, window);

		// Stand in for the MQTT server: ACK every delivered message on its own thread, as Paho's callback
		// thread does. This is the ONLY thing returning permits in this test.
		fakeClient.ackOnSeparateThread(tahuClient);
		startDrain();

		List<String> expected = new ArrayList<>();
		for (int i = 0; i < total; i++) {
			String topic = "topic/" + i;
			tahuClient.publish(topic, "x".getBytes(), 1, false);
			expected.add(topic);
		}

		// No releasePermits() anywhere - the buffer can only drain if ACKs are being processed
		awaitBufferDepth(0, TIMEOUT_MS * 4);

		Assert.assertEquals(fakeClient.publishedTopics(), expected, "Every message must be delivered, in publish order");
		awaitPermits(window, TIMEOUT_MS);
	}

	// ------------------------------------------------------------------------------------------------------------
	// Concurrency
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * Concurrent publishers must not lose or duplicate messages, and the buffer must account for every one of them.
	 * The publish-or-queue decision is made under a single lock precisely so this holds.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 4)
	public void concurrentPublishersDoNotLoseMessages() throws Exception {
		final int threads = 8;
		final int perThread = 50;
		wire(0, 4096);
		tahuClient.setPublishBufferCapacity(threads * perThread);

		final CountDownLatch startGate = new CountDownLatch(1);
		final CountDownLatch finished = new CountDownLatch(threads);
		final AtomicInteger failures = new AtomicInteger();

		for (int t = 0; t < threads; t++) {
			final int threadIndex = t;
			Thread thread = new Thread(() -> {
				try {
					startGate.await();
					for (int i = 0; i < perThread; i++) {
						tahuClient.publish("topic/" + threadIndex + "/" + i, "x".getBytes(), 1, false);
					}
				} catch (Throwable e) {
					failures.incrementAndGet();
				} finally {
					finished.countDown();
				}
			}, "publisher-" + t);
			thread.setDaemon(true);
			thread.start();
		}

		startGate.countDown();
		Assert.assertTrue(finished.await(TIMEOUT_MS * 3, TimeUnit.MILLISECONDS), "Publishers did not finish - possible deadlock");
		Assert.assertEquals(failures.get(), 0, "No publisher should have failed");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), threads * perThread,
				"Every message must be accounted for in the buffer");

		startDrain();
		releasePermits(threads * perThread);
		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishedTopics().size(), threads * perThread, "Every message must be delivered");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Disconnect and the LWT (IMM-5460)
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * IMM-5460. The LWT is the only death certificate on a clean DISCONNECT, because the MQTT server suppresses the
	 * Will. With the in-flight window exhausted the acknowledged publish would be buffered - and the buffer is torn
	 * down moments later - so it must fall back to QoS 0 and actually leave the process.
	 *
	 * Asserts the fallback rather than the disconnect ordering: it calls publishLwt() directly, so it does not need a
	 * connected Paho client to tear down.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void lwtFallsBackToQos0WhenItCannotBePublishedAtItsConfiguredQos() throws Exception {
		wire(0, 8);
		configureLwt(1);

		tahuClient.publishLwt(false);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(LWT_TOPIC),
				"The LWT must reach the MQTT client even with no in-flight permit available");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0,
				"The LWT must not be left sitting in the buffer - the fallback exists so it is not queued at all");
		Assert.assertEquals(availablePermits(), 0, "QoS 0 must not consume an in-flight permit");
	}

	/** With a permit free there is nothing to fall back from, so the configured QoS is used and acknowledged. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void lwtUsesItsConfiguredQosWhenAPermitIsAvailable() throws Exception {
		wire(4, 8);
		configureLwt(1);

		tahuClient.publishLwt(false);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(LWT_TOPIC));
		Assert.assertEquals(availablePermits(), 3, "A QoS 1 LWT must take a permit, so it can be acknowledged");
	}

	/**
	 * IMM-5460 / IMM-5458. The drain thread is started by connect() before the Paho client is built, so a disconnect
	 * that finds no client still has one to stop. Moving the shutdown below the LWT publish put it inside the
	 * 'client != null' arm, which is exactly how this path would have been left orphaned.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void disconnectStopsTheDrainThreadWhenThereIsNoClient() throws Exception {
		wire(0, 8);
		startDrain();
		Assert.assertNotNull(get(tahuClient, "publishBufferDrainThread"), "Precondition: the drain thread is running");

		set(tahuClient, "client", null);
		tahuClient.disconnect(0, 1, false, false, false);

		Assert.assertNull(get(tahuClient, "publishBufferDrain"), "The drain must be stopped by a no-client disconnect");
		Assert.assertNull(get(tahuClient, "publishBufferDrainThread"), "The drain thread reference must be cleared");
	}

	/**
	 * The drain wait is what lets the LWT keep its configured QoS. With the buffer already empty it returns at once and
	 * the LWT goes inline at QoS 1, taking a permit so it can be acknowledged - no fallback.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void disconnectPublishesTheLwtAtItsConfiguredQosWhenTheBufferIsClear() throws Exception {
		wire(4, 8);
		configureLwt(1);

		tahuClient.disconnect(0, 1, true, true, false);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(LWT_TOPIC));
		Assert.assertTrue(fakeClient.disconnectSent, "A delivered LWT must still be followed by a clean DISCONNECT");
	}

	/**
	 * IMM-5460 last resort. Every route to publishing the death certificate has failed, so a clean DISCONNECT would
	 * suppress the Will and leave subscribers with nothing. The connection must drop instead, so the MQTT server
	 * publishes the Will it already holds from connect().
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void disconnectSuppressesTheDisconnectPacketWhenTheLwtCannotBePublished() throws Exception {
		wire(4, 8);
		configureLwt(1);
		fakeClient.failNextPublishes(Integer.MAX_VALUE);

		tahuClient.disconnect(0, 1, true, true, false);

		Assert.assertTrue(fakeClient.disconnectForciblyCalled, "The disconnect must still complete");
		Assert.assertFalse(fakeClient.disconnectSent,
				"With no death certificate published, the DISCONNECT must be withheld so the Will fires");
	}

	/** A client with no LWT configured has nothing to escalate over, so the disconnect stays clean. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void disconnectSendsTheDisconnectPacketWhenThereIsNoLwt() throws Exception {
		wire(4, 8);

		tahuClient.disconnect(0, 1, true, true, false);

		Assert.assertTrue(fakeClient.disconnectSent, "No LWT configured is not a failure to publish one");
	}

	/**
	 * Nothing buffered before a disconnect may be published on the next session.
	 *
	 * The guarantee is real but indirect: connect() clears nothing itself, it relies on calling disconnect() first,
	 * and disconnect() clears the buffer only as a side effect of stopping the drain thread. Since
	 * startPublishBufferDrainThread() returns early when a drain already exists, any future path that reached
	 * connect() without a disconnect would carry the old buffer and its live drain thread straight into the new
	 * session and replay stale messages against it. This pins the invariant at the disconnect boundary.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferedMessagesAreNotReplayedOnTheNextSession() throws Exception {
		wire(0, 8);
		startDrain();
		for (int i = 0; i < 5; i++) {
			tahuClient.publish("topic/stale-" + i, "x".getBytes(), 1, false);
		}
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 5, "Precondition: the messages are queued");
		Object drainBeforeDisconnect = get(tahuClient, "publishBufferDrain");
		Assert.assertNotNull(drainBeforeDisconnect, "Precondition: a drain thread owns that buffer");

		tahuClient.disconnect(0, 1, false, false, false);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0, "The disconnect must leave nothing queued");
		Assert.assertNull(get(tahuClient, "publishBufferDrain"), "The drain that owned the old buffer must be gone");

		// Stand in for the next connect(): the server is up, permits are restored and the drain is restarted,
		// exactly as connect() does. The session has to be live or the drain would decline to publish anyway and
		// the assertion below would prove nothing.
		fakeClient.markConnected();
		wire(8, 8);
		startDrain();
		Assert.assertNotSame(get(tahuClient, "publishBufferDrain"), drainBeforeDisconnect,
				"The new session must get a new drain, not the one still holding the old buffer");

		// Give the new drain longer than its poll interval to publish anything it might still be holding
		Thread.sleep(DRAIN_POLL_MS * 3);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0);
		Assert.assertEquals(fakeClient.publishedTopics(), List.of(),
				"No message queued before the disconnect may reach the MQTT server on the next session");
	}

	/**
	 * Once the drain is gone, a message that would be buffered is refused rather than accepted.
	 *
	 * The client still reports connected from the drain shutdown until disconnectForcibly(), so publish() used to
	 * accept into a buffer nothing owned - a concurrent publisher, an AsyncPublisher thread, or publishLwt(), which
	 * has call sites outside disconnect(). Those entries could then be inherited by the next session's drain and
	 * flushed onto it; a retained death certificate replayed that way tells subscribers a live host is offline.
	 *
	 * Refusing is the honest answer: publish() returning null means "queued, not lost", which cannot be true of a
	 * message no thread will ever send. The throw reaches the caller in time for it to store the message itself.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void publishIsRefusedWhenNoDrainOwnsTheBuffer() throws Exception {
		wire(0, 8);
		invoke(tahuClient, "shutdownPublishBufferDrainThread");
		Assert.assertNull(get(tahuClient, "publishBufferDrain"), "Precondition: nothing owns the buffer now");

		try {
			tahuClient.publish(LWT_TOPIC, "death".getBytes(), 1, true);
			Assert.fail("A publish with no drain to send it must reach the caller as a failure, not return null");
		} catch (TahuException e) {
			Assert.assertTrue(e.getMessage().contains("no publish buffer drain"), "Unexpected message: " + e);
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0, "A refused message must not be buffered");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 1,
				"A refusal the caller was told about is a rejection, counted with the capacity refusals");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 0,
				"Nothing was accepted, so nothing was discarded");
	}

	/**
	 * A shutdown discards the buffer whether or not a drain is still registered.
	 *
	 * Belt and braces behind publishIsRefusedWhenNoDrainOwnsTheBuffer: that refusal is what keeps entries out of an
	 * unowned buffer, and this is what guarantees any that got there anyway do not survive into the next session.
	 * The discard used to sit below the publishBufferDrain == null guard, so the second shutdown - the one
	 * connect() performs before every attempt - returned without clearing.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void shutdownDiscardsTheBufferEvenWithNoDrainRegistered() throws Exception {
		wire(0, 8);
		tahuClient.publish(LWT_TOPIC, "death".getBytes(), 1, true);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "Precondition: the message is queued");

		// Drop the drain reference without touching the buffer, as the teardown window leaves it.
		set(tahuClient, "publishBufferDrain", null);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "Precondition: the buffer outlived its drain");

		invoke(tahuClient, "shutdownPublishBufferDrainThread");

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0,
				"A shutdown must discard the buffer whether or not a drain is still registered");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 1,
				"A discarded message is lost data and must be counted - publish() told the caller it was queued");

		// Stand in for the new session, as bufferedMessagesAreNotReplayedOnTheNextSession does.
		fakeClient.markConnected();
		wire(8, 8);
		Thread.sleep(DRAIN_POLL_MS * 3);

		Assert.assertEquals(fakeClient.publishedTopics(), List.of(),
				"Nothing queued in the old session may reach the MQTT server on the next one");
	}

	/** Discards are a lifetime total for the client, so they accumulate across sessions rather than resetting. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void discardedMessagesAccumulateOnTheLifetimeCounter() throws Exception {
		wire(0, 8);
		startDrain();
		for (int i = 0; i < 3; i++) {
			tahuClient.publish("topic/first-session-" + i, "x".getBytes(), 1, false);
		}
		invoke(tahuClient, "shutdownPublishBufferDrainThread");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 3);

		fakeClient.markConnected();
		wire(0, 8);
		startDrain();
		for (int i = 0; i < 2; i++) {
			tahuClient.publish("topic/second-session-" + i, "x".getBytes(), 1, false);
		}
		invoke(tahuClient, "shutdownPublishBufferDrainThread");
		Assert.assertEquals(tahuClient.getPublishBufferDiscardedMessageCount(), 5,
				"The counter is for the life of the client, not the life of a session");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 0,
				"Nothing was refused at the door here - the buffer never reached capacity");
	}

	/**
	 * A QoS 0 publish must not wait for the buffer to drain.
	 *
	 * QoS 0 takes no permit and is never buffered, so nothing about it depends on the acknowledged queue - holding it
	 * behind one makes live data stale for no delivery benefit. Every Sparkplug publish in tahu is QoS 0, and
	 * EdgeClient publishes while holding its own clientLock, so a QoS 0 publisher stuck behind the drain stalls the
	 * whole Sparkplug path including sequence number allocation.
	 *
	 * Timing-based by necessity, with a wide margin: the drain holds the MQTT client for 5 x 200ms here, and the
	 * assertion only fails if the QoS 0 publish waited for more than a couple of those.
	 */
	@Test(
			timeOut = TIMEOUT_MS * 2)
	public void qos0DoesNotWaitForTheBufferToDrain() throws Exception {
		wire(0, 8);
		for (int i = 0; i < 5; i++) {
			tahuClient.publish("topic/queued-" + i, "x".getBytes(), 1, false);
		}
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 5, "Precondition: the drain has work to do");

		fakeClient.setPublishDelay(200);
		releasePermits(8);
		// Let the drain get inside a send, so the QoS 0 publish arrives while the client is genuinely busy
		Thread.sleep(100);

		long start = System.currentTimeMillis();
		tahuClient.publish("topic/live", "x".getBytes(), 0, false);
		long elapsed = System.currentTimeMillis() - start;

		// The two regimes are far apart: taking publishOrderLock measured 500-904ms across runs, against a drain
		// holding the client for 5 x 200ms. Bypassing it costs only this message's own send - a flat 201ms here.
		Assert.assertTrue(elapsed < 450, "QoS 0 waited " + elapsed + "ms for the buffer to drain - it must not queue "
				+ "behind acknowledged traffic, and the wait scales with buffer depth");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Byte capacity
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * The buffer is bounded by bytes as well as by message count.
	 *
	 * A count alone does not bound heap, which is the dimension that OOMs: payloads are held by reference and sized
	 * by the caller, so the default 10,000 messages was 10,000 x an unknown. Measured before this bound existed, 100
	 * x 1MB QoS 1 publishes were admitted without complaint and retained ~200MB.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferRejectsAtItsByteCapacity() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferByteCapacity(1000);

		tahuClient.publish("topic/a", new byte[400], 1, false);
		tahuClient.publish("topic/b", new byte[400], 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 800, "Precondition: both are queued and accounted");

		try {
			tahuClient.publish("topic/c", new byte[400], 1, false);
			Assert.fail("A publish past the byte budget must throw so the caller can store the message");
		} catch (TahuException e) {
			Assert.assertTrue(e.getMessage().contains("byte capacity"), "Unexpected message: " + e.getMessage());
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 2, "The rejected message must not be buffered");
		Assert.assertTrue(tahuClient.getPublishBufferDepth() < tahuClient.getPublishBufferCapacity(),
				"The count bound must not be what rejected it - that would prove nothing about the byte bound");
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 800, "A rejected payload must not be accounted for");
		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 1,
				"A byte-budget refusal is a rejection: the caller was told and can still store it");
	}

	/** One payload larger than the whole budget is refused on its own terms, not as a full buffer. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void payloadLargerThanTheWholeBudgetIsRejectedOutright() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferByteCapacity(1000);

		try {
			tahuClient.publish("topic/huge", new byte[2000], 1, false);
			Assert.fail("A payload that can never fit must be rejected rather than queued");
		} catch (TahuException e) {
			Assert.assertTrue(e.getMessage().contains("exceeds the whole publish buffer budget"),
					"The message must name the payload, not the buffer state: " + e.getMessage());
		}

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0, "Nothing was queued");
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 0, "Nothing was accounted for");
	}

	/**
	 * The byte total tracks the buffer exactly, including across a failed send.
	 *
	 * This is the invariant the whole bound rests on. A total that drifts above the truth does not fail where it
	 * drifted - it fails later, as a client refusing every publish against a buffer that looks empty, which is the
	 * same shape as an in-flight permit drifting from Paho's window. The requeue path is where drift would start,
	 * since a failed send removes an entry from the head and puts it back.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferBytesReturnToZeroAcrossAFailedSend() throws Exception {
		wire(0, 8);
		tahuClient.publish("topic/first", new byte[100], 1, false);
		tahuClient.publish("topic/second", new byte[100], 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 200);

		fakeClient.failNextPublishes(1);
		releasePermits(8);
		awaitBufferDepth(0);

		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 0,
				"An empty buffer must account for zero bytes - a failed send removes and re-adds its message");
	}

	/** The disconnect discard clears the byte total with the messages it throws away. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void discardingTheBufferClearsTheByteTotal() throws Exception {
		wire(0, 8);
		tahuClient.publish("topic/a", new byte[100], 1, false);
		tahuClient.publish("topic/b", new byte[100], 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 200, "Precondition: both are accounted for");

		invoke(tahuClient, "shutdownPublishBufferDrainThread");

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 0);
		Assert.assertEquals(tahuClient.getPublishBufferBytes(), 0,
				"A discarded buffer must release its byte budget, or the client refuses publishes forever after");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Async publishes
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * A retrying async publish must occupy one buffer slot, however many attempts its retry budget allows.
	 *
	 * The retry loop had no success break, and handlePublish() could not tell it there had been one - it discarded
	 * publishOrBuffer()'s return and swallowed the exception. Under backpressure the first attempt buffered the
	 * message and every later attempt appended another copy of it, so numAttempts copies of one BIRTH or DATA
	 * message were delivered when permits returned. Identical copies, same payload, so no subscriber can tell them
	 * apart from a real rebirth.
	 *
	 * Not reachable before this branch: the first attempt blocked in semaphore.acquire() and never returned to the
	 * loop - the deadlock this work removed was hiding the missing break.
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void asyncPublishWithRetryBuffersOneCopyOfTheMessage() throws Exception {
		wire(0, 8);

		tahuClient.asyncPublish("topic/async", "x".getBytes(), 1, false, true, 20, 3);
		Thread.sleep(400);

		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1,
				"One logical message must occupy one buffer slot, however many retry attempts it was given");
	}

	/**
	 * A rejected async publish is still retried, since that is the outcome retrying exists for.
	 *
	 * The buffer is full, so nothing holds the message and each attempt is a genuine new one. This is what stops the
	 * success break above from turning "retry" into "try once".
	 */
	@Test(
			timeOut = TIMEOUT_MS)
	public void asyncPublishRetriesWhenTheBufferRejectsIt() throws Exception {
		wire(0, 8);
		tahuClient.setPublishBufferCapacity(1);
		tahuClient.publish("topic/filler", "x".getBytes(), 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "Precondition: the buffer is at capacity");

		tahuClient.asyncPublish("topic/async", "x".getBytes(), 1, false, true, 20, 3);
		Thread.sleep(400);

		Assert.assertEquals(tahuClient.getPublishBufferRejectedMessageCount(), 3,
				"Every attempt against a full buffer is a real attempt and must be counted as a rejection");
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1, "A rejected message must not be buffered");
	}

	// ------------------------------------------------------------------------------------------------------------
	// Harness
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * Puts the client into the state connect() would leave it in, but with a fake Paho client and a chosen number of
	 * free permits. The drain thread is NOT started - tests that need it call startDrain() so they control timing.
	 */
	/**
	 * Stands in for connect(): a live session, and a drain thread that owns the buffer.
	 *
	 * The drain is started here rather than left to each test because connect() starts it before the Paho client
	 * exists, so a client that reports connected always has one. bufferPublish() now refuses a message when it does
	 * not - see publishIsRefusedWhenNoDrainOwnsTheBuffer - and a harness that buffered without one was modelling a
	 * state the client cannot be in. Tests that want the buffer to sit still wire 0 permits, which keeps the drain
	 * parked on its acquire.
	 */
	private void wire(int availablePermits, int maxInflight) throws Exception {
		set(tahuClient, "client", fakeClient);
		set(tahuClient, "semaphore", new Semaphore(availablePermits, true));
		set(tahuClient, "lockedMessageSet", ConcurrentHashMap.newKeySet());
		set(tahuClient, "maxInFlightMessages", maxInflight);
		startDrain();
	}

	private static final String LWT_TOPIC = "spBv1.0/G1/NDEATH/E1";

	private void configureLwt(int qos) throws Exception {
		set(tahuClient, "lwtTopic", LWT_TOPIC);
		set(tahuClient, "lwtPayload", "death".getBytes());
		set(tahuClient, "lwtQoS", qos);
		set(tahuClient, "lwtRetain", false);
	}

	private void startDrain() throws Exception {
		invoke(tahuClient, "startPublishBufferDrainThread");
	}

	private void releasePermits(int count) throws Exception {
		semaphore().release(count);
	}

	private int availablePermits() throws Exception {
		return semaphore().availablePermits();
	}

	private Semaphore semaphore() throws Exception {
		return (Semaphore) get(tahuClient, "semaphore");
	}

	private void awaitPermits(int expected, long timeoutMs) throws Exception {
		long deadline = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < deadline) {
			if (availablePermits() == expected) {
				return;
			}
			Thread.sleep(10);
		}
		Assert.fail("Permits never returned to " + expected + " (currently " + availablePermits() + ")");
	}

	private void awaitBufferDepth(int expected) throws Exception {
		awaitBufferDepth(expected, TIMEOUT_MS - 500);
	}

	private void awaitBufferDepth(int expected, long timeoutMs) throws Exception {
		long deadline = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < deadline) {
			if (tahuClient.getPublishBufferDepth() == expected) {
				// Let the final in-flight send settle before asserting on what was published
				Thread.sleep(50);
				if (tahuClient.getPublishBufferDepth() == expected) {
					return;
				}
			}
			Thread.sleep(10);
		}
		Assert.fail("Buffer depth never reached " + expected + " (currently " + tahuClient.getPublishBufferDepth() + ")");
	}

	private static TahuClient newTahuClient() throws Exception {
		return new TahuClient(new MqttClientId("test-client", false), new MqttServerName("test-server"),
				new MqttServerUrl("tcp://localhost:1883"), null, null, true, 30, new NoOpCallback(), null, false);
	}

	private static void set(Object target, String fieldName, Object value) throws Exception {
		Field field = findField(target.getClass(), fieldName);
		field.setAccessible(true);
		field.set(target, value);
	}

	private static Object get(Object target, String fieldName) throws Exception {
		Field field = findField(target.getClass(), fieldName);
		field.setAccessible(true);
		return field.get(target);
	}

	private static void invoke(Object target, String methodName) throws Exception {
		Method method = target.getClass().getDeclaredMethod(methodName);
		method.setAccessible(true);
		method.invoke(target);
	}

	private static Field findField(Class<?> type, String name) throws NoSuchFieldException {
		for (Class<?> c = type; c != null; c = c.getSuperclass()) {
			try {
				return c.getDeclaredField(name);
			} catch (NoSuchFieldException ignored) {
				// keep walking up
			}
		}
		throw new NoSuchFieldException(name);
	}

	/**
	 * A Paho client that records what it was asked to publish and can be told to fail on demand. Nothing touches a
	 * network.
	 */
	private static class FakeMqttClient extends TahuMqttAsyncClient {

		private final List<String> published = new ArrayList<>();
		private final AtomicInteger attempts = new AtomicInteger();
		private final AtomicInteger failuresRemaining = new AtomicInteger();
		private final AtomicBoolean connected = new AtomicBoolean(true);
		private final AtomicInteger nextMessageId = new AtomicInteger(1);
		private volatile long publishDelayMs;
		private volatile TahuClient ackTarget;
		private volatile ExecutorService ackExecutor;
		private volatile boolean disconnectSent;
		private volatile boolean disconnectForciblyCalled;

		private FakeMqttClient() throws MqttException {
			super("tcp://localhost:1883", "test-client", null);
		}

		/**
		 * Makes every successful publish be ACKed asynchronously, mirroring the MQTT server plus Paho's callback
		 * thread. Deliberately on another thread: deliveryComplete() must take messageLock while the publisher may
		 * still hold publishOrderLock -> messageLock.
		 */
		private void ackOnSeparateThread(TahuClient target) {
			this.ackExecutor = Executors.newSingleThreadExecutor(runnable -> {
				Thread thread = new Thread(runnable, "fake-broker-ack");
				thread.setDaemon(true);
				return thread;
			});
			this.ackTarget = target;
		}

		private void shutdownAckThread() {
			ExecutorService executor = ackExecutor;
			ackTarget = null;
			if (executor != null) {
				executor.shutdownNow();
			}
		}

		/* Brings the fake back up, so a test can model a genuinely live next session. */
		private void markConnected() {
			connected.set(true);
		}

		/* Holds each send open, so a test can observe what a caller sees while the drain is mid-publish. */
		private void setPublishDelay(long millis) {
			publishDelayMs = millis;
		}

		private void failNextPublishes(int count) {
			failuresRemaining.set(count);
		}

		private synchronized List<String> publishedTopics() {
			return new ArrayList<>(published);
		}

		private int publishAttempts() {
			return attempts.get();
		}

		@Override
		public boolean isConnected() {
			return connected.get();
		}

		/*
		 * Recorded rather than performed. The real methods would throw on a client that never connected, and the
		 * sendDisconnectPacket flag is the observable for the IMM-5460 Will escalation.
		 */
		@Override
		public void disconnectForcibly(long quiesceTimeout, long disconnectTimeout, boolean sendDisconnectPacket) {
			this.disconnectSent = sendDisconnectPacket;
			this.disconnectForciblyCalled = true;
			connected.set(false);
		}

		@Override
		public void close() {
			// no-op - nothing to release
		}

		@Override
		public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained)
				throws MqttException {
			attempts.incrementAndGet();
			long delay = publishDelayMs;
			if (delay > 0) {
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new MqttException(e);
				}
			}
			if (failuresRemaining.get() > 0) {
				failuresRemaining.decrementAndGet();
				// 32202 == REASON_CODE_MAX_INFLIGHT, the realistic transient failure
				throw new MqttException(32202);
			}
			synchronized (this) {
				published.add(topic);
			}

			FakeDeliveryToken token = new FakeDeliveryToken(nextMessageId.getAndIncrement());
			TahuClient target = ackTarget;
			ExecutorService executor = ackExecutor;
			if (target != null && executor != null && qos > 0 && !executor.isShutdown()) {
				try {
					executor.submit(() -> target.deliveryComplete(token));
				} catch (RejectedExecutionException ignored) {
					// shutting down
				}
			}
			return token;
		}
	}

	/** Minimal IMqttDeliveryToken - only getMessageId() is meaningful to TahuClient. */
	private static class FakeDeliveryToken implements IMqttDeliveryToken {

		private final int messageId;
		private Object userContext;
		private IMqttActionListener actionCallback;

		private FakeDeliveryToken(int messageId) {
			this.messageId = messageId;
		}

		@Override
		public int getMessageId() {
			return messageId;
		}

		@Override
		public MqttMessage getMessage() {
			return null;
		}

		@Override
		public void waitForCompletion() {
		}

		@Override
		public void waitForCompletion(long timeout) {
		}

		@Override
		public boolean isComplete() {
			return true;
		}

		@Override
		public MqttException getException() {
			return null;
		}

		@Override
		public void setActionCallback(IMqttActionListener listener) {
			this.actionCallback = listener;
		}

		@Override
		public IMqttActionListener getActionCallback() {
			return actionCallback;
		}

		@Override
		public IMqttAsyncClient getClient() {
			return null;
		}

		@Override
		public String[] getTopics() {
			return new String[0];
		}

		@Override
		public void setUserContext(Object userContext) {
			this.userContext = userContext;
		}

		@Override
		public Object getUserContext() {
			return userContext;
		}

		@Override
		public int[] getGrantedQos() {
			return new int[0];
		}

		@Override
		public boolean getSessionPresent() {
			return false;
		}

		@Override
		public MqttWireMessage getResponse() {
			return null;
		}
	}

	private static class NoOpCallback implements ClientCallback {

		@Override
		public void shutdown() {
		}

		@Override
		public void messageArrived(MqttServerName mqttServerName, MqttServerUrl mqttServerUrl, MqttClientId clientId,
				String rawTopic, MqttMessage message) {
		}

		@Override
		public void connectionLost(MqttServerName mqttServerName, MqttServerUrl mqttServerUrl, MqttClientId clientId,
				Throwable cause) {
		}

		@Override
		public void connectComplete(boolean reconnect, MqttServerName mqttServerName, MqttServerUrl mqttServerUrl,
				MqttClientId clientId) {
		}
	}
}
