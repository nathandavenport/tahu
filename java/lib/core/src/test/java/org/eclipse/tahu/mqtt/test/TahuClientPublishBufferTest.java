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
			invoke(tahuClient, "shutdownPublishBufferDrainThread");
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
		Assert.assertEquals(availablePermits(), 0);

		// The Paho callback thread returning the permit - this is the call that used to block forever
		final CountDownLatch done = new CountDownLatch(1);
		Thread callbackThread = new Thread(() -> {
			tahuClient.deliveryComplete(token);
			done.countDown();
		}, "fake-MQTT-Call");
		callbackThread.setDaemon(true);
		callbackThread.start();

		Assert.assertTrue(done.await(2, TimeUnit.SECONDS), "deliveryComplete() blocked - the deadlock has returned");
		Assert.assertEquals(availablePermits(), 1, "The permit must have been returned");
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

	/** A buffered message that fails is retried, then dropped once its attempts are exhausted. */
	@Test(
			timeOut = TIMEOUT_MS)
	public void bufferedSendFailureIsRetriedThenDropped() throws Exception {
		wire(0, 8);

		tahuClient.publish("topic/poison", "x".getBytes(), 1, false);
		Assert.assertEquals(tahuClient.getPublishBufferDepth(), 1);

		fakeClient.failNextPublishes(Integer.MAX_VALUE);
		startDrain();
		releasePermits(8);

		awaitBufferDepth(0);
		Assert.assertEquals(fakeClient.publishAttempts(), 3, "Should stop after MAX_BUFFERED_PUBLISH_ATTEMPTS");
		Assert.assertEquals(availablePermits(), 8, "Every permit taken for a failed send must be returned");
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
	// Harness
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * Puts the client into the state connect() would leave it in, but with a fake Paho client and a chosen number of
	 * free permits. The drain thread is NOT started - tests that need it call startDrain() so they control timing.
	 */
	private void wire(int availablePermits, int maxInflight) throws Exception {
		set(tahuClient, "client", fakeClient);
		set(tahuClient, "semaphore", new Semaphore(availablePermits, true));
		set(tahuClient, "lockedMessageSet", ConcurrentHashMap.newKeySet());
		set(tahuClient, "maxInFlightMessages", maxInflight);
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
		private volatile TahuClient ackTarget;
		private volatile ExecutorService ackExecutor;

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

		@Override
		public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained)
				throws MqttException {
			attempts.incrementAndGet();
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
