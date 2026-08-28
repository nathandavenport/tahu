/********************************************************************************
 * Copyright (c) 2022-2023 Cirrus Link Solutions and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Cirrus Link Solutions - initial implementation
 ********************************************************************************/

package org.eclipse.tahu.mqtt;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.util.Debug;
import org.eclipse.tahu.exception.TahuErrorCode;
import org.eclipse.tahu.exception.TahuException;
import org.eclipse.tahu.message.model.StatePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An Custom MQTT client.
 */
public class TahuClient implements MqttCallbackExtended {

	private static Logger logger = LoggerFactory.getLogger(TahuClient.class.getName());

	private static final long DEFAULT_CONNECT_RETRY_INTERVAL = 1000;
	private static final long DEFAULT_CONNECT_MONITOR_INTERVAL = 10000;
	private static final long DEFAULT_CONNECT_ATTEMPT_TIMEOUT = 30000;
	private static final int DEFAULT_PUBLISH_BUFFER_CAPACITY = 10000;
	private static final long PUBLISH_BUFFER_POLL_INTERVAL = 250;
	/*
	 * The retry budget for a buffered publish that fails to send, as a window of wall clock rather than a count of
	 * attempts. The failures worth retrying - REASON_CODE_MAX_INFLIGHT when Tahu's permit count has drifted above
	 * Paho's window, REASON_CODE_CLIENT_NOT_CONNECTED across a brief drop - are transient and clear on their own, so
	 * the budget only means anything if it spans time. An attempt count alone did not: nothing on the failure path
	 * delays, so three attempts were consumed in about two milliseconds and a blip destroyed the message.
	 *
	 * The window is deliberately short, because it is calibrated to a race and not to an outage. 32202 is thrown when
	 * Tahu's permit count is ahead of Paho's actualInFlight, and the two go back into step as soon as Paho finishes
	 * the acknowledgement it is already processing - microseconds, not seconds. Where the drift is structural rather
	 * than a race the condition never clears on its own, and where the client is simply not connected the drain's own
	 * isConnected() check handles it on the next pass; in neither case does a longer window help, it only holds up
	 * everything else in the buffer, because the backoff sleeps the drain thread. At 500ms a message gets three
	 * attempts spread over roughly 750ms of wall clock.
	 *
	 * MAX_BUFFERED_PUBLISH_ATTEMPTS remains as a backstop against a message that fails instantly in a tight loop
	 * filling the log; the window is what normally decides.
	 */
	private static final long MAX_BUFFERED_PUBLISH_RETRY_WINDOW = 500;
	private static final int MAX_BUFFERED_PUBLISH_ATTEMPTS = 10;
	private static final long MAX_BUFFERED_PUBLISH_RETRY_DELAY = 500;

	/*
	 * How long disconnect() will wait for the publish buffer to drain before publishing the LWT. Short on purpose:
	 * disconnect() runs per client, so a transmitter backed by a pool pays this serially, and it only elapses when
	 * the MQTT server is genuinely stuck. At 2 seconds a three client pool adds at most 6 seconds to a module stop.
	 */
	private static final long LWT_DRAIN_TIMEOUT = 2000;

	private Thread connectRunnableThread;
	private ConnectRunnable connectRunnable;
	private long connectRetryInterval;
	private long connectAttemptTimeout;

	/*
	 * Tracks the state of the connection attempts.
	 */
	private ConnectingState state = new ConnectingState();

	/*
	 * birth/death properties
	 */
	private boolean useSparkplugStatePayload;
	private Long lastStateDeathPayloadTimestamp;
	private String birthTopic;
	private byte[] birthPayload;
	private boolean birthRetain;
	private String lwtTopic;
	private byte[] lwtPayload;
	private int lwtQoS;
	private boolean lwtRetain;
	private IMqttDeliveryToken lwtDeliveryToken;
	private Object lwtDeliveryLock = new Object();

	/*
	 * The Asynchronous MQTT Client and MQTTConnectOptions
	 */
	private volatile TahuMqttAsyncClient client = null;
	MqttConnectOptions connectOptions = null;

	/*
	 * Other standard MQTT parameters.
	 */
	private final MqttServerUrl mqttServerUrl;
	private final MqttServerName mqttServerName;
	private final MqttClientId clientId;
	private String username;
	private String password;
	private final boolean cleanSession;
	private final int keepAlive;

	/*
	 * The callback client
	 */
	private ClientCallback callback;

	/**
	 * A list of topics the client has subscribed on
	 */
	private final SortedMap<String, Integer> subscriptions = new ConcurrentSkipListMap<>();

	/*
	 * Odds/ends
	 */
	private boolean autoReconnect;
	private RandomStartupDelay randomStartupDelay;

	/*
	 * The maximum number of in-flight (pending) messages for the client to store. If this maximum is, publishes will
	 * fail with and INTERNAL_ERROR: Caused by: org.eclipse.paho.client.mqttv3.MqttException: Too many publishes in
	 * progress
	 */
	private int maxInFlightMessages = 32768;

	/*
	 * The maximum number of topics per individual subscribe message.
	 */
	private int maxTopicsPerSubscribe = 256;

	/*
	 * The maximum number of messages held in the FIFO publish buffer.
	 *
	 * A QoS > 0 publish needs an in-flight permit, and permits are only returned by deliveryComplete() when the MQTT
	 * server ACKs a message. Rather than waiting for one - which is what deadlocked this client against
	 * deliveryComplete() - a message that cannot get a permit immediately is buffered and sent by the drain thread once
	 * a permit frees up. Once anything is buffered, every subsequent QoS > 0 message is buffered too, so they leave the
	 * buffer in submission order on the happy path.
	 *
	 * That ordering is BEST EFFORT and is not a guarantee. See publishOrderLock below for where it does not hold, and
	 * note that MQTT itself only orders messages when at most one is in flight - with maxInFlightMessages in the
	 * thousands, acknowledged messages can already be reordered on the wire regardless of anything this class does.
	 *
	 * QoS 0 is never buffered - it needs no permit and is always published immediately, so it can overtake buffered
	 * QoS > 0 messages during a stall.
	 *
	 * The buffer MUST stay bounded. An unbounded one only moves the failure from thread exhaustion to heap exhaustion
	 * under a sustained server stall. At capacity the newest publish is rejected with a TahuException so the caller can
	 * fall back to its own store-and-forward rather than lose the message silently.
	 */
	private volatile int publishBufferCapacity = DEFAULT_PUBLISH_BUFFER_CAPACITY;

	/*
	 * The FIFO publish buffer, the lock guarding both it and publish ordering, and the thread that drains it.
	 *
	 * publishOrderLock guards the decision "publish now or queue" so that, in normal operation, no message overtakes one
	 * already queued.
	 *
	 * KNOWN GAP, accepted deliberately: when a buffered send fails, the lock is released between the failed publish and
	 * the requeueOrDrop() that puts the message back at the head, so a concurrent publisher can see an empty buffer and
	 * go inline ahead of it. Ordering is therefore best effort, not guaranteed. This is accepted rather than fixed
	 * because MQTT provides no ordering guarantee across an in-flight window greater than one anyway, and the callers
	 * that care about sequence - Sparkplug NDATA/DDATA - publish at QoS 0 and never enter this buffer.
	 *
	 * Lock order is always publishOrderLock -> messageLock; deliveryComplete() takes messageLock only, so there is no
	 * cycle and permits can always be returned.
	 */
	private final Object publishOrderLock = new Object();
	private final Deque<BufferedPublish> publishBuffer = new ArrayDeque<>();
	/*
	 * Cumulative count of publishes rejected because the buffer was full. Guarded by publishOrderLock like the
	 * buffer itself. Never reset while the client lives - it is a lifetime counter, not a gauge, so a consumer
	 * can see that loss happened even if the buffer has since drained.
	 */
	private long publishBufferRejectedMessageCount;

	/*
	 * Counted separately from publishBufferRejectedMessageCount, because the two mean different things to whoever is
	 * looking at them. A rejection is a refusal at the door: publish() threw, so the caller knows and can store the
	 * message itself. A discard is a message this client accepted - publish() returned null, which the contract
	 * defines as "queued, not lost" - and then threw away. Only the second is a loss the caller could not see coming.
	 */
	private long publishBufferDiscardedMessageCount;

	private PublishBufferDrain publishBufferDrain;
	private Thread publishBufferDrainThread;

	private Date connectTime;
	private Date disconnectTime;
	private Date onlineDate;
	private Date offlineDate;
	private double totalUptime;
	private double totalDowntime;
	private int connectionCount = 0; // # of Edge Nodes connected to this MQTT Client's Broker
	private boolean doLatencyCheck = false;
	private long numMesgsArrived = 0;
	private long lastNumMesgsArrived = 0;

	/*
	 * Read by publish() to tell an interrupt that is part of an intentional disconnect from one that has silently
	 * dropped a message, so it is written and read from different threads.
	 */
	private volatile boolean disconnectInProgress = false;

	/*
	 * IMM-5460. Set by publishLwt() so disconnect() can tell whether the death certificate actually reached the MQTT
	 * client. False means every attempt failed, including the QoS 0 fallback - and the only remaining way to get one
	 * published is to let the MQTT server publish the Will, which means NOT sending a DISCONNECT packet.
	 */
	private volatile boolean lwtPublishSucceeded = true;

	private Object clientLock = new Object();
	private ConnectionMonitorThread connectionMonitorThread;

	private boolean trackFirstConnection = false;
	private boolean firstConnection = true;
	private boolean resubscribed = false;

	// Whether or not the BIRTH should be published on connect and controls the STATE of the client
	private boolean onlineState;

	// volatile: written in connect() under messageLock, but read under publishOrderLock in publishOrBuffer() and
	// with no lock at all by the publish buffer drain thread. Without volatile there is no happens-before edge to
	// those readers, so a reconnect's replacement instance may not be seen promptly.
	private volatile Semaphore semaphore;
	private volatile Set<Integer> lockedMessageSet;
	private final Object messageLock;

	public TahuClient(final MqttClientId clientId, final MqttServerName mqttServerName,
			final MqttServerUrl mqttServerUrl, final String username, final String password, boolean cleanSession,
			int keepAlive, ClientCallback callback, RandomStartupDelay randomStartupDelay, boolean onlineState) {
		this.mqttServerUrl = mqttServerUrl;
		this.mqttServerName = mqttServerName;
		this.clientId = clientId;
		this.username = username;
		this.password = password;
		this.cleanSession = cleanSession;
		this.keepAlive = keepAlive;
		this.callback = callback;
		this.randomStartupDelay = randomStartupDelay;
		this.lwtRetain = false;
		this.birthRetain = false;
		this.autoReconnect = true;
		this.setConnectRetryInterval(DEFAULT_CONNECT_RETRY_INTERVAL);
		this.setConnectAttemptTimeout(DEFAULT_CONNECT_ATTEMPT_TIMEOUT);
		this.renewDisconnectTime();
		this.renewOnlineDate();
		this.renewOfflineDate();
		this.onlineState = onlineState;
		this.messageLock = new Object();
	}

	public TahuClient(final MqttClientId clientId, final MqttServerName mqttServerName,
			final MqttServerUrl mqttServerUrl, String username, String password, boolean cleanSession, int keepAlive,
			ClientCallback callback, RandomStartupDelay randomStartupDelay, boolean useSparkplugStatePayload,
			String birthTopic, byte[] birthPayload, String lwtTopic, byte[] lwtPayload, int lwtQoS,
			boolean onlineState) {
		this(clientId, mqttServerName, mqttServerUrl, username, password, cleanSession, keepAlive, callback,
				randomStartupDelay, onlineState);
		this.setLifecycleProps(useSparkplugStatePayload, birthTopic, birthPayload, false, lwtTopic, lwtPayload, lwtQoS,
				false);
	}

	public TahuClient(final MqttClientId clientId, final MqttServerName mqttServerName,
			final MqttServerUrl mqttServerUrl, String username, String password, boolean cleanSession, int keepAlive,
			ClientCallback callback, RandomStartupDelay randomStartupDelay, boolean onlineState,
			boolean useSparkplugStatePayload, String birthTopic, byte[] birthPayload, boolean birthRetain,
			String lwtTopic, byte[] lwtPayload, int lwtQoS, boolean lwtRetain) {
		this(clientId, mqttServerName, mqttServerUrl, username, password, cleanSession, keepAlive, callback,
				randomStartupDelay, onlineState);
		this.setLifecycleProps(useSparkplugStatePayload, birthTopic, birthPayload, birthRetain, lwtTopic, lwtPayload,
				lwtQoS, lwtRetain);
	}

	/**
	 * Sets the properties relating to client life cycle events such as LWT and Birth topics and payloads.
	 * 
	 * @param birthTopic the topic to publish birth certificates on
	 * @param birthPayload the payload of a birth certificate
	 * @param birthRetain whether to retain birth certificate messages
	 * @param lwtTopic the topic to publish LWT on
	 * @param lwtPayload the payload of an LWT
	 * @param lwtRetain whether to retain LWT messages
	 */
	private void setLifecycleProps(boolean useSparkplugStatePayload, String birthTopic, byte[] birthPayload,
			boolean birthRetain, String lwtTopic, byte[] lwtPayload, int lwtQoS, boolean lwtRetain) {
		this.useSparkplugStatePayload = useSparkplugStatePayload;
		this.birthTopic = birthTopic;
		this.birthPayload = birthPayload;
		this.birthRetain = birthRetain;
		this.lwtTopic = lwtTopic;
		this.lwtPayload = lwtPayload;
		this.lwtQoS = lwtQoS;
		this.lwtRetain = lwtRetain;

	}

	protected MqttConnectOptions getMqttConnectOptions() {
		return connectOptions;
	}

	protected void setMqttConnectOptions(MqttConnectOptions connectOptions) {
		this.connectOptions = connectOptions;
	}

	public int getAvailablePublishPermits() {
		return semaphore != null ? semaphore.availablePermits() : 0;
	}

	public long getNumMesgsArrived() {
		return numMesgsArrived;
	}

	public long getMesgsArrivedDelta() {
		// Returns the number of messages arrived since last called.
		long delta = numMesgsArrived - lastNumMesgsArrived;
		lastNumMesgsArrived = numMesgsArrived;
		return delta;
	}

	public void clearMesgArrivedCount() {
		numMesgsArrived = 0;
		lastNumMesgsArrived = 0;
	}

	public void setMaxInflightMessages(int max) {
		this.maxInFlightMessages = max;
	}

	public int getMaxInflightMessages() {
		return this.maxInFlightMessages;
	}

	public void setPublishBufferCapacity(int publishBufferCapacity) {
		this.publishBufferCapacity = publishBufferCapacity;
	}

	public int getPublishBufferCapacity() {
		return this.publishBufferCapacity;
	}

	public void setDoLatencyCheck(boolean state) {
		doLatencyCheck = state;
	}

	public boolean getDoLatencyCheck() {
		return doLatencyCheck;
	}

	public void clearConnectionCount() {
		connectionCount = 0;
	}

	public void incrementConnectionCount() {
		connectionCount++;
	}

	public int getConnectionCount() {
		return connectionCount;
	}

	public MqttServerUrl getMqttServerUrl() {
		return mqttServerUrl;
	}

	public MqttServerName getMqttServerName() {
		return mqttServerName;
	}

	public MqttClientId getClientId() {
		return clientId;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassord(String password) {
		this.password = password;
	}

	public int getKeepAlive() {
		return keepAlive;
	}

	public boolean isCleanSession() {
		return cleanSession;
	}

	public Map<String, Integer> getSubscriptions() {
		return Collections.unmodifiableMap(subscriptions);
	}

	public int getMaxTopicsPerSubscribe() {
		return maxTopicsPerSubscribe;
	}

	public void setMaxTopicsPerSubscribe(int maxTopicsPerSubscribe) {
		this.maxTopicsPerSubscribe = maxTopicsPerSubscribe;
	}

	public ClientCallback getCallback() {
		// If callback is null, return a no-op implementation
		return this.callback != null ? this.callback : new ClientCallback() {
			@Override
			public void shutdown() {
				return;
			}

			@Override
			public void messageArrived(MqttServerName mqttServerName, MqttServerUrl mqttServerUrl,
					MqttClientId clientId, String topic, MqttMessage message) {
			}

			@Override
			public void connectionLost(MqttServerName mqttServerName, MqttServerUrl mqttServerUrl,
					MqttClientId clientId, Throwable cause) {
			}

			@Override
			public void connectComplete(boolean reconnect, MqttServerName mqttServerName, MqttServerUrl mqttServerUrl,
					MqttClientId clientId) {
			}
		};
	}

	public void setAutoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
	}

	public boolean getAutoReconnect() {
		return autoReconnect;
	}

	public String getLwtTopic() {
		return lwtTopic;
	}

	public void setLwtRetain(boolean retain) {
		this.lwtRetain = retain;
	}

	public boolean getLwtRetain() {
		return lwtRetain;
	}

	public Long getLastStateDeathPayloadTimestamp() {
		return lastStateDeathPayloadTimestamp;
	}

	public boolean isConnected() {
		if (client != null) {
			return client.isConnected();
		} else {
			return false;
		}
	}

	public boolean isConnectedAndResubscribed() {
		if (client != null) {
			return client.isConnected() && resubscribed;
		} else {
			return false;
		}
	}

	public long getConnectDuration() throws TahuException {
		if (getConnectTime() != null) {
			Date now = new Date();
			return now.getTime() - getConnectTime().getTime();
		} else if (getDisconnectTime() != null) {
			Date now = new Date();
			return -(now.getTime() - getDisconnectTime().getTime());
		} else {
			throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "Connect time is unknown");
		}
	}

	/**
	 * Returns the availability as a percentage, calculated by uptime/(uptime+downtime).
	 * 
	 * @return a double representing the percentage of availability
	 * @throws TahuException
	 */
	public double getAvailability() throws TahuException {
		if (getConnectTime() != null) {
			Date now = new Date();
			totalUptime = totalUptime + (now.getTime() - getConnectTime().getTime());
		}
		if (getDisconnectTime() != null) {
			Date now = new Date();
			totalDowntime = totalDowntime + (now.getTime() - getDisconnectTime().getTime());
		}

		if ((totalUptime + totalDowntime == 0)) {
			throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "Connect time is unknown");
		}

		return (totalUptime / (totalUptime + totalDowntime)) * 100.0;
	}

	public void resetAvailability() {
		totalUptime = 0;
		totalDowntime = 0;
	}

	public int getBufferedMessageCount() {
		try {
			return client.getBufferedMessageCount();
		} catch (Exception e) {
			logger.debug("Failed to get the MQTT Client Buffered Message Count", e);
			return 0;
		}
	}

	public int getInFlightMessageCount() {
		try {
			return client.getInFlightMessageCount();
		} catch (Exception e) {
			logger.debug("Failed to get the MQTT Client In-Flight Message Count", e);
			return 0;
		}
	}

	public Debug getDebug() {
		try {
			return client.getDebug();
		} catch (Exception e) {
			logger.debug("Failed to get the MQTT Client Debug info", e);
			return null;
		}
	}

	public Properties getClientCommsDebug() {
		if (client != null) {
			return client.getClientCommsDebug();
		} else {
			return null;
		}
	}

	public Properties getClientStateDebug() {
		if (client != null) {
			return client.getClientStateDebug();
		} else {
			return null;
		}
	}

	public Properties getConOptions() {
		if (client != null) {
			return client.getConOptions();
		} else {
			return null;
		}
	}

	public Date getClientInitDateTime() {
		return client.getClientInitDateTime();
	}

	/**
	 * Returns a {@link Date} instance representing the online date.
	 * 
	 * @return the online date.
	 */
	public Date getOnlineDateTime() {
		return this.onlineDate;
	}

	/**
	 * Renews the online date.
	 */
	public void renewOnlineDate() {
		this.onlineDate = new Date();
	}

	/**
	 * Returns a {@link Date} instance representing the offline date.
	 * 
	 * @return the offline date.
	 */
	public Date getOfflineDateTime() {
		return this.offlineDate;
	}

	/**
	 * Renews the offline date.
	 */
	public void renewOfflineDate() {
		this.offlineDate = new Date();
	}

	/**
	 * Publishes a message, or buffers it in FIFO order if it cannot be published immediately.
	 *
	 * A QoS > 0 message needs an in-flight permit. Rather than waiting for one - which is what deadlocked this client
	 * against {@link #deliveryComplete(IMqttDeliveryToken)} - a message that cannot get a permit immediately is
	 * appended to the publish buffer and sent later by the drain thread.
	 *
	 * Once the buffer is non-empty every subsequent QoS > 0 message is appended to it until it drains, so buffered
	 * messages are sent in submission order while sends keep succeeding. That ordering is BEST EFFORT: it is not held
	 * across a failed send, and MQTT does not order messages across an in-flight window greater than one in any case.
	 * Callers that need a strict sequence must carry it in the payload.
	 *
	 * QoS 0 is never buffered - it takes no permit and is always sent immediately, so it may overtake queued QoS > 0
	 * messages while the in-flight window is exhausted.
	 *
	 * @return the delivery token if the message was published inline, or <b>null</b> if it was buffered. A null return
	 *         means the message is queued, not lost - callers must not treat it as a failure.
	 * @throws TahuException if the client is not usable, the publish itself failed, or the buffer is full
	 */
	public IMqttDeliveryToken publish(String topic, byte[] payload, int qos, boolean retained) throws TahuException {
		try {
			if (client == null) {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " is null");
			} else if (client.isConnected()) {
				return publishOrBuffer(topic, payload, qos, retained);
			} else {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " is not connected");
			}
		} catch (TahuException e) {
			/*
			 * No InterruptedException handler here, deliberately. 3.x added one to distinguish an interrupt that is part
			 * of an intentional disconnect from one that silently dropped a message - but that only mattered while
			 * publish() could block. The interruptible semaphore.acquire() is gone, replaced by a non-blocking
			 * tryAcquire() plus the publish buffer, so nothing in the try above can throw InterruptedException and Java
			 * would reject a catch for it. The failure mode that change guarded against cannot arise on this path.
			 *
			 * Its underlying concern does still apply, in a new form: publish() returns null when a message is
			 * buffered, and callers that treat null as failure - or ignore it - can lose a BIRTH or an LWT. That is
			 * tracked separately, not papered over here.
			 */
			throw e;
		} catch (Exception e) {
			throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
		}
	}

	/**
	 * Decides - atomically with respect to the buffer - whether a message is published now or queued.
	 *
	 * The whole decision runs under {@link #publishOrderLock} so that a message cannot overtake one already queued -
	 * except across a failed buffered send, which that field documents as an accepted gap. It
	 * NEVER waits for a permit while holding a lock: the inline path uses a non-blocking {@link Semaphore#tryAcquire()}
	 * and falls back to buffering. That is what keeps {@link #deliveryComplete(IMqttDeliveryToken)} - which needs only
	 * {@link #messageLock} - able to return permits at all times.
	 *
	 * Lock order is always publishOrderLock -> messageLock. deliveryComplete() takes messageLock only, so there is no
	 * cycle.
	 */
	private IMqttDeliveryToken publishOrBuffer(String topic, byte[] payload, int qos, boolean retained)
			throws TahuException {
		/*
		 * QoS 0 takes no permit and is never subject to backpressure, so it is always sent immediately - it is never
		 * buffered, even while QoS > 0 messages are queued. This means a QoS 0 message CAN overtake buffered QoS > 0
		 * messages during a stall. That is deliberate: MQTT only orders within a QoS level, and holding live
		 * fire-and-forget data behind a stalled acknowledged queue would make it stale for no delivery benefit.
		 *
		 * Deliberately OUTSIDE publishOrderLock, and it has to be for any of the above to be true. The drain thread
		 * holds that monitor across client.publish() for every buffered message, and Java monitors are not fair, so a
		 * QoS 0 publisher that took it would queue behind the drain rather than slot in between its iterations -
		 * measured at 500-900ms against a five message backlog, and it scales with buffer depth. This path touches
		 * neither publishBuffer nor semaphore, only the volatile client, so it needs no lock at all. Every Sparkplug
		 * publish is QoS 0, and EdgeClient publishes while holding its own clientLock, so a QoS 0 publisher stalled
		 * here stalls the whole Sparkplug path with it - sequence number allocation included.
		 */
		if (qos == 0) {
			// Read once: disconnect() nulls the field without holding publishOrderLock, and holding it here would
			// not have helped anyway.
			final TahuMqttAsyncClient publishClient = client;
			try {
				logger.debug("{}: Publishing with QoS0 on {}, Payload size = {}", getClientId(), topic, payload.length);
				return publishClient.publish(topic, payload, qos, retained);
			} catch (Throwable t) {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR, t);
			}
		}

		synchronized (publishOrderLock) {
			// Order preservation among QoS > 0: anything queued means this message queues behind it
			if (!publishBuffer.isEmpty()) {
				bufferPublish(topic, payload, qos, retained, "buffer is draining");
				return null;
			}

			final Semaphore permitHolder = semaphore;
			if (permitHolder == null) {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "MQTT client: " + clientId.getMqttClientId()
						+ " has no in-flight window - it has never connected");
			}

			// Non-blocking on purpose - never wait for a permit while holding a lock
			if (!permitHolder.tryAcquire()) {
				bufferPublish(topic, payload, qos, retained, "no in-flight permit available");
				return null;
			}

			logger.trace("{}: Took permit in publish - available permits remaining: {}", getClientId(),
					permitHolder.availablePermits());
			return publishWithPermit(permitHolder, topic, payload, qos, retained);
		}
	}

	/**
	 * Publishes a message for which an in-flight permit has already been taken.
	 *
	 * Ownership of the permit transfers to this method: on success it belongs to
	 * {@link #deliveryComplete(IMqttDeliveryToken)}, and on failure it is released here. Callers must not release it
	 * themselves.
	 *
	 * Must be called holding {@link #publishOrderLock}.
	 */
	private IMqttDeliveryToken publishWithPermit(Semaphore permitHolder, String topic, byte[] payload, int qos,
			boolean retained) throws TahuException {
		// Reentrant - every caller already holds this. Taken explicitly so publish ordering cannot be bypassed.
		synchronized (publishOrderLock) {
			boolean handedOff = false;
			try {
				synchronized (messageLock) {
					logger.debug("{}: Publishing with QoS{} on {}, Payload size = {}", getClientId(), qos, topic,
							payload.length);
					IMqttDeliveryToken token = client.publish(topic, payload, qos, retained);
					lockedMessageSet.add(token.getMessageId());
					handedOff = true;
					return token;
				}
			} catch (Throwable t) {
				logger.error("{}: Failed to publish on {} - releasing permit - available permits: {}", getClientId(),
						topic, permitHolder.availablePermits(), t);
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR, t);
			} finally {
				// Release only if the message never reached the MQTT client. Once it has, the permit belongs to
				// deliveryComplete(). If lockedMessageSet.add() is what failed, releasing here is still correct -
				// deliveryComplete()'s remove() will return false and it will not release a second time.
				if (!handedOff) {
					permitHolder.release();
				}
			}
		}
	}

	/**
	 * Appends a message to the publish buffer. Must be called holding {@link #publishOrderLock}.
	 *
	 * The buffer is bounded. An unbounded one would simply move the original failure mode from thread exhaustion to
	 * heap exhaustion under a sustained server stall, so at capacity the newest message is rejected and the caller is
	 * told - letting it fall back to its own store-and-forward rather than silently losing data.
	 *
	 * INVARIANT: only QoS > 0 messages are ever buffered. The drain thread relies on this - it takes a permit before
	 * dequeuing, so a buffered QoS 0 message (which needs no permit and is never ACKed) would stall the buffer for as
	 * long as the in-flight window stayed exhausted. TahuClientPublishBufferTest#bufferNeverHoldsQos0Messages guards it.
	 *
	 * A message is also rejected when no drain thread owns the buffer, for the same reason as a full one: nothing
	 * would ever send it. That state is not reachable during a live session - connect() starts the drain before the
	 * Paho client exists, and publish() cannot get here with no client - so it means the disconnect is already
	 * tearing this client down. Accepting there would return null, which publish() defines as "queued, not lost",
	 * for a message certain to be discarded by the disconnect that is already running.
	 */
	private void bufferPublish(String topic, byte[] payload, int qos, boolean retained, String reason)
			throws TahuException {
		// Reentrant - every caller already holds this. Taken explicitly so the buffer can never be mutated without it.
		synchronized (publishOrderLock) {
			if (publishBufferDrain == null) {
				// A refusal, not a discard: the caller is told in time to store the message itself.
				publishBufferRejectedMessageCount++;
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " is disconnecting - no publish buffer drain "
								+ "thread to send a buffered message, rejecting publish on " + topic);
			}

			if (publishBuffer.size() >= getPublishBufferCapacity()) {
				publishBufferRejectedMessageCount++;
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
						"MQTT client: " + clientId.getMqttClientId() + " publish buffer is full ("
								+ publishBuffer.size() + "/" + getPublishBufferCapacity()
								+ ") - rejecting publish on " + topic);
			}

			publishBuffer.addLast(new BufferedPublish(topic, payload, qos, retained));
			if (publishBuffer.size() == 1) {
				logger.warn("{}: Buffering publish on {} - {} - subsequent messages will queue behind it",
						getClientId(), topic, reason);
			} else {
				logger.debug("{}: Buffering publish on {} - {} - buffer depth is now {}", getClientId(), topic, reason,
						publishBuffer.size());
			}
			publishOrderLock.notifyAll();
		}
	}

	/**
	 * @return the number of messages currently waiting in the publish buffer. 0 in normal operation - a sustained
	 *         non-zero value means the MQTT server is not acknowledging fast enough.
	 */
	public int getPublishBufferDepth() {
		synchronized (publishOrderLock) {
			return publishBuffer.size();
		}
	}

	/**
	 * @return the number of publishes refused because the publish buffer was already at capacity, for the life of
	 *         this client. Any non-zero value means the MQTT server stalled for longer than the buffer could absorb.
	 *         These are refusals rather than losses from the caller's point of view:
	 *         {@link #publish(String, byte[], int, boolean)} throws, so the caller knows immediately and can fall
	 *         back to its own store and forward. For messages this client accepted and then could not send, see
	 *         {@link #getPublishBufferDiscardedMessageCount()}.
	 */
	public long getPublishBufferRejectedMessageCount() {
		synchronized (publishOrderLock) {
			return publishBufferRejectedMessageCount;
		}
	}

	/**
	 * @return the number of buffered publishes this client accepted and then did not send, for the life of this
	 *         client. Every one of them is lost data the caller could not see coming, because
	 *         {@link #publish(String, byte[], int, boolean)} returned null - "queued, not lost" - before the loss
	 *         happened. Two routes reach it:
	 *         <ul>
	 *         <li>a buffered message dropped once its retry window is spent - see requeueOrDrop();</li>
	 *         <li>the whole buffer discarded at disconnect, counted by the number of entries cleared. Since
	 *         connect() disconnects first, an ordinary reconnect takes this route: a queued message must not be
	 *         replayed onto the next session, so it is discarded here and counted.</li>
	 *         </ul>
	 *         IMM-5456 covers giving the caller a way to store these rather than lose them.
	 */
	public long getPublishBufferDiscardedMessageCount() {
		synchronized (publishOrderLock) {
			return publishBufferDiscardedMessageCount;
		}
	}

	/*
	 * A message that could not be published immediately and is waiting its turn in the publish buffer.
	 */
	private static class BufferedPublish {
		private final String topic;
		private final byte[] payload;
		private final int qos;
		private final boolean retained;

		/*
		 * Send attempts made so far. Paho only retries messages it has accepted; a throw from its publish() means the
		 * message never entered its outbound queue or persistence, so nothing else will resend it.
		 */
		private int attempts;

		/* When this message first failed to send, so the retry budget can be measured in wall clock. */
		private long firstFailureTime;

		private BufferedPublish(String topic, byte[] payload, int qos, boolean retained) {
			this.topic = topic;
			this.payload = payload;
			this.qos = qos;
			this.retained = retained;
		}
	}

	private void startPublishBufferDrainThread() {
		synchronized (publishOrderLock) {
			if (publishBufferDrain != null) {
				return;
			}
			publishBufferDrain = new PublishBufferDrain();
			publishBufferDrainThread = new Thread(publishBufferDrain,
					"TahuPublishBufferDrain-" + getClientId().getMqttClientId());
			publishBufferDrainThread.setDaemon(true);
			publishBufferDrainThread.start();
			logger.debug("{}: Started the publish buffer drain thread", getClientId());
		}
	}

	/**
	 * Waits, bounded, for the publish buffer to drain.
	 *
	 * Called from disconnect() BEFORE the LWT is published, so the LWT can go out inline at its configured QoS and be
	 * acknowledged rather than queued behind the backlog into a buffer that is about to be torn down.
	 *
	 * Deliberately NOT called from publishLwt(). That method holds {@link #lwtDeliveryLock}, and
	 * {@link #deliveryComplete(IMqttDeliveryToken)} needs the same monitor before it can return an in-flight permit -
	 * so waiting there would block the very mechanism that drains the buffer, and the wait could never succeed. Here
	 * the calling thread holds only clientLock, which neither the drain thread nor deliveryComplete() takes.
	 */
	private void awaitPublishBufferDrained(long timeoutMillis) {
		int depth = getPublishBufferDepth();
		if (depth == 0) {
			return;
		}

		logger.info("{}: Waiting up to {}ms for {} buffered publishes to drain before publishing the LWT",
				getClientId(), timeoutMillis, depth);
		long deadline = System.currentTimeMillis() + timeoutMillis;
		while (System.currentTimeMillis() < deadline) {
			if (getPublishBufferDepth() == 0) {
				logger.debug("{}: Publish buffer drained before the LWT", getClientId());
				return;
			}
			try {
				Thread.sleep(PUBLISH_BUFFER_POLL_INTERVAL);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		logger.warn("{}: Publish buffer still holds {} messages after {}ms - the LWT will fall back to QoS 0, and "
				+ "anything still queued will be discarded", getClientId(), getPublishBufferDepth(), timeoutMillis);
	}

	/*
	 * Stops the drain thread without letting a failure abort a disconnect that is already in progress.
	 *
	 * Called from both arms of disconnect(): a client that was never created still has a drain thread to stop,
	 * because connect() starts it before the Paho client is built.
	 */
	private void shutdownPublishBufferDrainThreadQuietly() {
		try {
			shutdownPublishBufferDrainThread();
		} catch (Exception e) {
			logger.error("{}: Failed to shutdown publish buffer drain thread", getClientId());
		}
	}

	private void shutdownPublishBufferDrainThread() {
		final Thread drainThread;
		synchronized (publishOrderLock) {
			/*
			 * Discard FIRST, and unconditionally - above the drain == null guard, not below it.
			 *
			 * A message can reach the buffer after the drain has already been stopped: publish() admits anything
			 * while client.isConnected() is true, which it remains from here until disconnectForcibly(), including
			 * across the deliberate sleep in disconnect(). Nothing owns those entries. Discarding them below the
			 * guard meant the next disconnect - connect() calls one before every attempt, and by then the drain is
			 * already null - returned without clearing, so connect()'s new drain thread found a non-empty buffer and
			 * flushed the previous session's messages onto the new connection. For a death certificate that is a
			 * retained "offline" arriving on a live session, ahead of the BIRTH queued behind it.
			 */
			if (!publishBuffer.isEmpty()) {
				// Counted, not just logged: publish() told these callers the message was queued, not lost, so this
				// is the only place the loss can be reported to them - see getPublishBufferDiscardedMessageCount().
				publishBufferDiscardedMessageCount += publishBuffer.size();
				logger.warn("{}: Discarding {} buffered publishes on shutdown", getClientId(), publishBuffer.size());
				publishBuffer.clear();
			}
			publishOrderLock.notifyAll();

			if (publishBufferDrain == null) {
				return;
			}
			publishBufferDrain.setKeepRunning(false);
			drainThread = publishBufferDrainThread;
			publishBufferDrain = null;
			publishBufferDrainThread = null;
			// Again, now that keepRunning is false, so a drain parked in wait() sees the flag rather than relying on
			// the interrupt below to be what stops it.
			publishOrderLock.notifyAll();
		}
		if (drainThread != null) {
			drainThread.interrupt();
		}
	}

	/**
	 * Puts a failed send back at the head of the buffer, or drops it once its retry budget is spent.
	 *
	 * Paho does not rescue this message. Its QoS 1/2 redelivery only covers messages already accepted into its
	 * outbound queue and persistence, and a throw from its publish() means this one never got that far. If it is not
	 * re-queued here it is gone.
	 *
	 * The budget is a window of wall clock, not a count of attempts. The realistic failures - REASON_CODE_MAX_INFLIGHT
	 * (32202) when Tahu's permit count has drifted above Paho's window, or REASON_CODE_CLIENT_NOT_CONNECTED (32104) if
	 * the connection drops between the check and the send - are transient and clear on their own, which a count alone
	 * gave them no time to do.
	 *
	 * NOTE: the caller has already left the publishOrderLock block by the time this runs, so a concurrent publisher can
	 * slip in ahead of the message being re-queued. That reordering is an accepted gap - see publishOrderLock - so
	 * "back at the head" means ahead of everything still queued, not ahead of everything published after the failure.
	 *
	 * @return how long the caller should wait before the next attempt, or 0 if the message was dropped. The caller
	 *         must do the waiting: this method holds publishOrderLock, and sleeping here would block every publisher
	 *         on this client for the duration of the backoff.
	 */
	private long requeueOrDrop(BufferedPublish attempted, Throwable cause) {
		if (attempted == null) {
			return 0;
		}

		synchronized (publishOrderLock) {
			long now = System.currentTimeMillis();
			if (attempted.firstFailureTime == 0) {
				attempted.firstFailureTime = now;
			}
			attempted.attempts++;
			long elapsed = now - attempted.firstFailureTime;

			if (elapsed < MAX_BUFFERED_PUBLISH_RETRY_WINDOW && attempted.attempts < MAX_BUFFERED_PUBLISH_ATTEMPTS) {
				publishBuffer.addFirst(attempted);
				long delay = Math.min(PUBLISH_BUFFER_POLL_INTERVAL * attempted.attempts,
						MAX_BUFFERED_PUBLISH_RETRY_DELAY);
				logger.warn("{}: Failed to send buffered publish on {} - re-queued at head, attempt {}, {}ms into a "
						+ "{}ms window, retrying in {}ms", getClientId(), attempted.topic, attempted.attempts, elapsed,
						MAX_BUFFERED_PUBLISH_RETRY_WINDOW, delay, cause);
				return delay;
			}

			// Counted, not just logged: publish() returned null for this message, so the caller was told it was
			// queued and has no other way to learn that it was not sent.
			publishBufferDiscardedMessageCount++;
			logger.error("{}: Dropping buffered publish on {} after {} attempts over {}ms", getClientId(),
					attempted.topic, attempted.attempts, elapsed, cause);
			return 0;
		}
	}

	/*
	 * Drains the publish buffer head first as in-flight permits become available. FIFO on the happy path only - a failed
	 * send can be overtaken while it is re-queued, which publishOrderLock documents as an accepted gap.
	 *
	 * Permits are acquired WITHOUT holding publishOrderLock - only the dequeue-and-publish step takes it - so this
	 * thread can never block a caller or deliveryComplete().
	 *
	 * Relies on the buffer holding only QoS > 0 messages - see bufferPublish().
	 */
	private class PublishBufferDrain implements Runnable {

		private volatile boolean keepRunning = true;

		private void setKeepRunning(boolean keepRunning) {
			this.keepRunning = keepRunning;
		}

		@Override
		public void run() {
			while (keepRunning) {
				try {
					synchronized (publishOrderLock) {
						while (keepRunning && publishBuffer.isEmpty()) {
							publishOrderLock.wait();
						}
						if (!keepRunning) {
							return;
						}
					}

					if (semaphore == null || client == null || !client.isConnected()) {
						// Nothing can be sent yet - leave the buffer intact and re-check shortly
						Thread.sleep(PUBLISH_BUFFER_POLL_INTERVAL);
						continue;
					}

					// Every buffered message is QoS > 0 - publishOrBuffer() sends QoS 0 straight to the MQTT
					// client and never queues it - so a permit is always required here.
					final Semaphore permitHolder = semaphore;
					if (!permitHolder.tryAcquire(PUBLISH_BUFFER_POLL_INTERVAL, TimeUnit.MILLISECONDS)) {
						continue;
					}

					boolean permitOwnershipTransferred = false;
					BufferedPublish attempted = null;
					long retryDelay = 0;
					try {
						synchronized (publishOrderLock) {
							BufferedPublish head = publishBuffer.peekFirst();
							if (head == null) {
								continue;
							}

							publishBuffer.removeFirst();
							attempted = head;
							permitOwnershipTransferred = true;
							publishWithPermit(permitHolder, head.topic, head.payload, head.qos, head.retained);

							attempted = null;
							if (publishBuffer.isEmpty()) {
								logger.info("{}: Publish buffer drained", getClientId());
							}
						}
					} catch (Throwable t) {
						/*
						 * The backoff is taken here rather than inside requeueOrDrop(), which holds publishOrderLock.
						 * It matters that something waits: after a failure every delay point in this loop is skipped -
						 * the buffer is non-empty so wait() does not fire, the client still reports connected so the
						 * poll sleep is not taken, and tryAcquire() returns at once because the permit the failed send
						 * just released is free again. Without this the whole retry window is spent in microseconds.
						 */
						retryDelay = requeueOrDrop(attempted, t);
					} finally {
						if (!permitOwnershipTransferred) {
							permitHolder.release();
						}
					}

					if (retryDelay > 0) {
						Thread.sleep(retryDelay);
					}
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					return;
				} catch (Throwable t) {
					logger.error("{}: Publish buffer drain thread error", getClientId(), t);
				}
			}
		}
	}

	public void asyncPublish(String topic, byte[] payload, int qos, boolean retained) throws TahuException {
		Thread t = new Thread(new AsyncPublisher(topic, payload, qos, retained, false, 0, 0));
		t.start();
	}

	public void asyncPublish(String topic, byte[] payload, int qos, boolean retained, boolean retry, long retryDelay,
			int numAttempts) throws TahuException {
		Thread t = new Thread(new AsyncPublisher(topic, payload, qos, retained, retry, retryDelay, numAttempts));
		t.start();
	}

	/**
	 * Subscribes to a topic.
	 * 
	 * @param topic the topic.
	 * @param qos the quality of service (0, 1, or 2)
	 * 
	 * @return the granted QoS for the subscription
	 * @throws TahuException
	 */
	public int subscribe(String topic, int qos) throws TahuException {
		synchronized (clientLock) {
			if (client != null) {
				if (client.isConnected()) {
					try {
						logger.debug("{}: server {} - Attempting to subscribe on topic {} with QoS={}", getClientId(),
								getMqttServerName(), topic, qos);
						IMqttToken token = client.subscribe(topic, qos);
						logger.trace("{}: Waiting for subscription on {}", getClientId(), topic);
						token.waitForCompletion();
						logger.trace("{}: Done waiting for subscription on {}", getClientId(), topic);
						subscriptions.put(topic, qos);
						int[] grantedQos = token.getGrantedQos();
						logger.debug("{}: Granted QoS for subcription on {}: {}", getClientId(), topic, grantedQos[0]);
						if (grantedQos != null && grantedQos.length == 1) {
							return grantedQos[0];
						} else {
							String errorMessage = getClientId() + ": server " + getMqttServerName()
									+ " - Failed to subscribe to " + topic;
							logger.error(errorMessage);
							throw new TahuException(TahuErrorCode.NOT_AUTHORIZED, errorMessage);
						}
					} catch (MqttException e) {
						logger.error(getClientId() + ": server " + getMqttServerName() + " - Failed to subscribe to "
								+ topic);
						throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
					}
				}
			}
			logger.debug("{}: Not connected and not subscribing to {} - just storing the subscription for now",
					getClientId(), topic);
			subscriptions.put(topic, qos);
			return qos;
		}
	}

	/**
	 * Subscribes to a set of topic.
	 * 
	 * @param topics the topics.
	 * @param qos the quality of service (0, 1, or 2)
	 * 
	 * @return the granted QoS levels for the subscriptions
	 * @throws TahuException
	 */
	public int[] subscribe(String[] topics, int[] qos) throws TahuException {
		synchronized (clientLock) {
			try {
				if (client != null) {
					if (client.isConnected()) {
						logger.debug("{}: Attempting to subscribe on topics {} with QoS={}", getClientId(), topics,
								qos);
						IMqttToken token = client.subscribe(topics, qos);
						logger.trace("{}: Waiting for subscription on {}", getClientId(), Arrays.toString(topics));
						token.waitForCompletion();
						logger.trace("{}: Done waiting for subscription on {}", getClientId(), Arrays.toString(topics));
						int[] grantedQos = token.getGrantedQos();
						if (grantedQos != null && grantedQos.length > 0) {
							for (int i = 0; i < topics.length; i++) {
								if (grantedQos[i] == qos[i]) {
									subscriptions.put(topics[i], qos[i]);
								} else {
									throw new TahuException(TahuErrorCode.NOT_AUTHORIZED,
											"Failed to subscribe to " + topics[i]);
								}
							}

							return grantedQos;
						} else {
							throw new TahuException(TahuErrorCode.NOT_AUTHORIZED, "Failed to subscribe to " + topics);
						}
					}
				}

				for (int i = 0; i < topics.length; i++) {
					subscriptions.put(topics[i], qos[i]);
				}
				logger.debug("{}: Not connected and not subscribing to {} - just storing the subscription for now",
						getClientId(), Arrays.asList(topics));
				return qos;
			} catch (Exception e) {
				throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
			}
		}
	}

	/**
	 * Unsubsribes from a topic.
	 * 
	 * @param topic the topic.
	 * @throws TahuException
	 */
	public void unsubscribe(String topic) throws TahuException {
		synchronized (clientLock) {
			if (client != null) {
				if (client.isConnected()) {
					try {
						logger.debug("{}: {} attempting to unsubscribe on topic {}", getClientId(), mqttServerName,
								topic);
						client.unsubscribe(topic);
					} catch (MqttException e) {
						throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
					}
				}
			}
			subscriptions.remove(topic);
		}
	}

	@Override
	public void connectionLost(Throwable cause) {
		logger.debug("{}: MQTT connectionLost() to {} :: {}", getClientId(), getMqttServerName(), getMqttServerUrl());
		if (logger.isTraceEnabled()) {
			if (client != null) {
				client.getDebug().dumpClientDebug();
			}
		}

		// reset the timers if needed
		if (getDisconnectTime() == null) {
			this.clearConnectTime();
			this.renewDisconnectTime();
			this.renewOfflineDate();
		}

		// Reset re-subscribed flag
		resubscribed = false;

		if (cause != null) {
			// We don't need to see all of the connection lost callbacks for clients
			logger.debug("{}: Connection lost due to {}", getClientId(), cause.getMessage(), cause);
		}

		// Trigger the connection lost event on the callback client
		getCallback().connectionLost(getMqttServerName(), getMqttServerUrl(), getClientId(), cause);
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		try {
			synchronized (lwtDeliveryLock) {
				if (lwtDeliveryToken != null && lwtDeliveryToken.getMessageId() == token.getMessageId()) {
					logger.info("{}: LWT Delivery complete for {}", getClientId(), token.getMessageId());
					lwtDeliveryToken = null;
				} else {
					logger.debug("{}: Delivery complete for {}", getClientId(), token.getMessageId());
				}
			}
		} catch (Throwable t) {
			logger.error("Failed to handle delivery complete for {}", token);
		} finally {
			synchronized (messageLock) {
				if (lockedMessageSet.remove(token.getMessageId())) {
					logger.trace("{}: Releasing permit - Available permits: {}", getClientId(),
							semaphore.availablePermits());
					semaphore.release();
				}
			}
		}
	}

	@Override
	public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
		logger.debug("{}: MQTT message arrived on topic {}", getClientId(), topic);
		numMesgsArrived++;
		getCallback().messageArrived(getMqttServerName(), getMqttServerUrl(), getClientId(), topic, mqttMessage);
	}

	/**
	 * Attempt to connect the TahuClient
	 */
	public void connect() {
		try {
			new URI(mqttServerUrl.getMqttServerUrl());
		} catch (Exception e) {
			logger.error("{}: Invalid MQTT Server URL: {}", getClientId(), mqttServerUrl.getMqttServerUrl());
			return;
		}

		logger.debug("{}: Starting new connect, autoReconnect: {}", getClientId(), autoReconnect);
		synchronized (clientLock) {
			logger.debug("{}: Got lock for new connect", getClientId());
			try {
				// reset the timers if needed
				if (getDisconnectTime() == null) {
					this.clearConnectTime();
					this.renewDisconnectTime();
				}

				if (getAutoReconnect() && state.inProgress()) {
					logger.debug("{}: Connect attempt already in progress", getClientId());
					return;
				} else {
					disconnect(0, 0, false, true);
					state.setInProgress(true);
					logger.debug("{}: Starting ConnectThread", getClientId());
					connectRunnable = new ConnectRunnable(this);
					connectRunnableThread = new Thread(connectRunnable);
					connectRunnableThread.start();
				}
			} catch (Throwable t) {
				logger.error("{}: Error connectiong", getClientId(), t);
			}
		}
	}

	public boolean isDisconnectInProgress() {
		return disconnectInProgress;
	}

	/**
	 * Attempt to disconnect the TahuClient.
	 * 
	 * @param retryConnect true if the client should attempt to reconnect.
	 */
	public void disconnect(long disconnectQuieseTime, long disconnectTimeout, boolean sendDisconnect,
			boolean waitForLwt) throws TahuException {
		this.disconnect(disconnectQuieseTime, disconnectTimeout, sendDisconnect, true, waitForLwt);
	}

	/**
	 * Attempt to disconnect the TahuClient.
	 * 
	 * @param retryConnect true if the client should attempt to reconnect.
	 */
	public void disconnect(long disconnectQuieseTime, long disconnectTimeout, boolean sendDisconnect,
			boolean publishLwt, boolean waitForLwt) throws TahuException {
		synchronized (clientLock) {
			disconnectInProgress = true;

			try {
				shutdownConnectionMonitorThread();
			} catch (Exception e) {
				logger.error("{}: Failed to shutdown connection monitor thread", getClientId());
			}

			try {
				if (connectRunnable != null && connectRunnableThread != null) {
					connectRunnable.stopConnectAttempts();
					connectRunnableThread.interrupt();
				}
			} catch (Exception e) {
				logger.error("{}: Failed to shut down the connect runnable", getClientId());
			}

			if (client != null) {
				boolean sendDisconnectPacket = sendDisconnect;
				try {
					/*
					 * IMM-5460 - give the buffer a bounded chance to drain BEFORE the LWT, so the LWT can go out
					 * inline at its configured QoS and be acknowledged. The drain thread is still running at this
					 * point; it is stopped below, after the LWT.
					 */
					awaitPublishBufferDrained(LWT_DRAIN_TIMEOUT);

					if (publishLwt) {
						/*
						 * A failed LWT publish must not abort the disconnect. Only MqttException is handled below, so
						 * letting a TahuException out here would skip disconnectForcibly() and close() while the
						 * finally block still nulls the client, leaking the Paho client with no way to reach it again.
						 */
						try {
							this.publishLwt(waitForLwt);
						} catch (Exception e) {
							logger.error("{}: Failed to publish the LWT during disconnect - continuing",
									getClientId(), e);
						}

						/*
						 * IMM-5460 - last resort. Every attempt to publish the death certificate failed, the QoS 0
						 * fallback included. A clean DISCONNECT would tell the MQTT server we left gracefully and
						 * suppress the Will, leaving subscribers with no death certificate at all - so drop the
						 * connection instead and let the server publish the Will it already holds from connect().
						 */
						if (sendDisconnectPacket && !lwtPublishSucceeded) {
							logger.warn("{}: Could not publish the LWT on {} by any route - closing without a "
									+ "DISCONNECT so the MQTT server publishes the Will instead", getClientId(),
									lwtTopic);
							sendDisconnectPacket = false;
						}
					}

					/*
					 * IMM-5460 - deliberately AFTER the LWT and BEFORE disconnectForcibly(). The drain thread has to
					 * be alive for a buffered LWT to reach the MQTT server at all, and dead before the client is
					 * closed, or it publishes into a client that is going away. It also clears the buffer, so
					 * anything still queued at this point is discarded - see IMM-5456.
					 */
					shutdownPublishBufferDrainThreadQuietly();

					// FIXME - remove This sleep is necessary due to:
					// https://github.com/eclipse/paho.mqtt.java/issues/850
					try {
						Thread.sleep(1000L);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					logger.debug("{}: Disconnecting...", getClientId());
					client.disconnectForcibly(disconnectQuieseTime, disconnectTimeout, sendDisconnectPacket);
					logger.debug("{}: Done disconecting", getClientId());
					client.close();
					logger.debug("{}: Client closed", getClientId());
				} catch (MqttException e) {
					throw new TahuException(TahuErrorCode.INTERNAL_ERROR, e);
				} finally {
					client = null;
					state.setInProgress(false);
					disconnectInProgress = false;
					lwtDeliveryToken = null;
					// Reset re-subscribed flag
					resubscribed = false;
				}
			} else {
				logger.debug("{}: Disconnect: Client is already null", getClientId());

				/*
				 * IMM-5460 - the drain thread is started by connect() before the Paho client is built, so a
				 * disconnect with no client still has one to stop. Without this it would be orphaned - the defect
				 * IMM-5458 describes, reached through a different door.
				 */
				shutdownPublishBufferDrainThreadQuietly();
			}

			// reset the timers if needed
			if (getDisconnectTime() == null) {
				this.clearConnectTime();
				this.renewDisconnectTime();
				this.renewOfflineDate();
			}

			disconnectInProgress = false;
		}
	}

	/*
	 * Attempt to connect.
	 */
	private IMqttToken attemptConnect(TahuMqttAsyncClient client, MqttConnectOptions options, String ctx)
			throws MqttSecurityException, MqttException {
		synchronized (clientLock) {
			if (isConnected()) {
				logger.trace("{} is already connected - not trying again", getClientId());
				return null;
			}
			if (randomStartupDelay != null && randomStartupDelay.isValid()) {
				long randomDelay = randomStartupDelay.getRandomDelay();
				logger.debug("{}: Waiting random delay of {} ms before reconnect attempt", getClientId(), randomDelay);
				try {
					Thread.sleep(randomDelay);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.warn("{}: Sleep interrupted", getClientId(), e);
				}
			}

			logger.debug("{}: Attempting {} to {}", getClientId(), ctx, getMqttServerUrl());
			logger.trace("{}: Thread {} :: {}", getClientId(), Thread.currentThread().getName(),
					Thread.currentThread().getId());

			// Make the call to connect (this is asynchronous)
			return client.connect(options, ctx, new IMqttActionListener() {

				@Override
				public void onSuccess(IMqttToken token) {
					logger.info("{}: {} succeeded", getClientId(), token.getUserContext());
					state.setInProgress(false);
				}

				@Override
				public void onFailure(IMqttToken token, Throwable throwable) {
					logger.warn("{}: {} failed due to {}", getClientId(), token.getUserContext(),
							throwable != null ? throwable.getMessage() : "?", throwable);
					logger.warn("{}: MQTT Client details: {}", getClientId(), getTahuClientDetails());
					state.setInProgress(false);
				}

				private String getTahuClientDetails() {
					StringBuilder sb = new StringBuilder();
					sb.append("MQTT Server Name = ").append(mqttServerName).append(" :: ");
					sb.append("MQTT Server URL = ").append(mqttServerUrl).append(" :: ");
					sb.append("MQTT Client ID = ").append(clientId).append(" :: ");
					sb.append("Using Birth = ").append(birthTopic == null || birthTopic.isEmpty() ? "false" : "true")
							.append(" :: ");
					sb.append("Using LWT = ").append(lwtTopic == null || lwtTopic.isEmpty() ? "false" : "true");
					return sb.toString();
				}
			});
		}
	}

	/**
	 * A class for tracking the connect in-progress state.
	 */
	private class ConnectingState {

		private boolean inProgress = false;

		protected void setInProgress(boolean inProgress) {
			this.inProgress = inProgress;
		}

		protected boolean inProgress() {
			return this.inProgress;
		}
	}

	/**
	 * A Runnable implementation for connecting the client to a broker. Will continue to attempt to connect on failure
	 * until the client is disconnected (setting the keepConnected flag to false).
	 */
	protected class ConnectRunnable implements Runnable {

		private MqttCallback callback;

		private boolean attemptConnects = true;

		public ConnectRunnable(final MqttCallback callback) {
			this.callback = callback;
		}

		public void stopConnectAttempts() {
			attemptConnects = false;
		}

		@Override
		public void run() {
			// ensure we are disconnected and null
			synchronized (clientLock) {
				if (client != null) {
					try {
						if (client.isConnected()) {
							client.disconnectForcibly(0, 1, false);
							shutdownConnectionMonitorThread();
						}
						// client.setCallback(null);
						client.close();
					} catch (MqttException e) {
						logger.error("{}: Error while disconnecting client", getClientId(), e);
					} finally {
						client = null;
					}
				}
			}

			try {
				// Reset re-subscribed flag
				resubscribed = false;

				if (connectOptions == null) {
					connectOptions = new MqttConnectOptions();
				}
				connectOptions.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
				connectOptions.setCleanSession(cleanSession);
				connectOptions.setConnectionTimeout(30);
				if (getUsername() != null && !getUsername().trim().isEmpty()) {
					logger.debug("{}: Setting username to {}", getClientId(), getUsername());
					connectOptions.setUserName(getUsername());
				}
				if (getPassword() != null && !getPassword().trim().isEmpty()) {
					logger.debug("{}: Setting password to ****", getClientId());
					connectOptions.setPassword(getPassword().toCharArray());
				}
				connectOptions.setKeepAliveInterval(keepAlive);
				if (lwtTopic != null) {
					if (useSparkplugStatePayload) {
						ObjectMapper mapper = new ObjectMapper();
						lastStateDeathPayloadTimestamp = new Date().getTime();
						StatePayload statePayload = new StatePayload(false, lastStateDeathPayloadTimestamp);
						logger.debug("{}: Setting Sparkplug WILL on {} with retain={} and payload={}", getClientId(),
								lwtTopic, lwtRetain, statePayload);
						byte[] payload = mapper.writeValueAsString(statePayload).getBytes();
						connectOptions.setWill(lwtTopic, payload, MqttOperatorDefs.QOS1, lwtRetain);
					} else {
						logger.debug("{}: Setting WILL on {} with retain={}", getClientId(), lwtTopic, lwtRetain);
						connectOptions.setWill(lwtTopic, lwtPayload, MqttOperatorDefs.QOS1, lwtRetain);
					}
				}
				logger.trace("Setting max in-flight messages to {}", getMaxInflightMessages());
				connectOptions.setMaxInflight(getMaxInflightMessages());
				synchronized (messageLock) {
					semaphore = new Semaphore(getMaxInflightMessages(), true);
					lockedMessageSet = ConcurrentHashMap.newKeySet();
				}
				startPublishBufferDrainThread();

				// Create the client instance
				logger.info("{}: Creating the MQTT Client to {} on thread {}", getClientId(), getMqttServerUrl(),
						Thread.currentThread().getName());
				client = new TahuMqttAsyncClient(getMqttServerUrl().toString(), getClientId().toString(), null);

				// Set the callback handler
				client.setCallback(callback);
				IMqttToken connectToken = null;

				// A time stamp to track the current attempt in case the underlying client is stuck attempting forever
				long attemptTimestamp = System.currentTimeMillis();

				if (autoReconnect) {
					try {
						while (!isConnected() && attemptConnects) {
							try {
								synchronized (clientLock) {
									if (!attemptConnects) {
										logger.info("{}: No longer attempting to connect", getClientId());
										state.setInProgress(false);
										return;
									}

									connectToken = attemptConnect(client, connectOptions, "connect with retry");

									// Update time stamp for current attempt
									attemptTimestamp = System.currentTimeMillis();
								}

								// Sleep for the connect retry interval
								Thread.sleep(getConnectRetryInterval());
							} catch (InterruptedException ie) {
								Thread.currentThread().interrupt();
								logger.info("{}: Connect thread {} interrupted - giving up",
										Thread.currentThread().getName(), getClientId());
								return;
							} catch (MqttException e) {
								if (e.getReasonCode() == MqttException.REASON_CODE_CONNECT_IN_PROGRESS) {
									if (connectToken != null) {
										logger.debug("{}: Still trying to connect - isComplete? {}, sessionPresent? {}",
												getClientId(), connectToken.isComplete(),
												connectToken.getSessionPresent());
									} else {
										logger.debug("{}: Still trying to connect", getClientId());
									}

									// Check if the connect attempt has timed out
									if (System.currentTimeMillis() - attemptTimestamp > connectAttemptTimeout) {
										synchronized (clientLock) {
											// Forcibly close the client
											logger.warn("{}: Connect attempt has timed out - forcing close",
													getClientId());
											client.close(true);
										}
									} else {
										Thread.sleep(500);
									}
								} else {
									logger.debug("{}: Unable to connect due to {}, next connect attempt in {} ms",
											getClientId(), e.getMessage(), getConnectRetryInterval());
									Thread.sleep(getConnectRetryInterval());
								}
							}
						}

						logger.info("{}: MQTT Client connected to {} on thread {}", getClientId(), getMqttServerUrl(),
								Thread.currentThread().getName());
						state.setInProgress(false);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						logger.info("{}: Connect thread 2 interrupted - giving up", getClientId());
						state.setInProgress(false);
						return;
					} catch (Throwable throwable) {
						logException(
								"Error while attempting connect (with autoReconnect=true) to " + getMqttServerUrl(),
								throwable);
						state.setInProgress(false);
						if (autoReconnect && !isConnected() && attemptConnects) {
							attemptRecovery();
						}
					}
				} else {
					try {
						synchronized (clientLock) {
							if (!attemptConnects) {
								logger.info("{}: No longer attempting to connect", getClientId());
								state.setInProgress(false);
								return;
							}

							// Attempt to connect
							attemptConnect(client, connectOptions, "connect");
						}
					} catch (Throwable throwable) {
						logException(
								"Error while attempting connect (with autoReconnect=false) to " + getMqttServerUrl(),
								throwable);
					}
				}
			} catch (Exception e) {
				logger.error("{}: Error while connecting client", getClientId(), e);
				state.setInProgress(false);
				if (autoReconnect && !isConnected() && attemptConnects) {
					attemptRecovery();
				}
			}
		}
	}

	private void attemptRecovery() {
		logger.warn("{}: Connect failed - retrying", getClientId());
		try {
			if (randomStartupDelay != null && randomStartupDelay.isValid()) {
				long randomDelay = randomStartupDelay.getRandomDelay();
				logger.info("{}: Sleeping {} before reconnect attempt", getClientId(), randomDelay);
				Thread.sleep(randomDelay);
			} else {
				Thread.sleep(getConnectRetryInterval());
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			logger.warn("{}: InterruptedException while preparing to reconnect", getClientId(), ie);
			return;
		}
		if (autoReconnect) {
			connect();
		} else {
			logger.warn("{}: AutoReconnect canceled - No longer going to retry", getClientId());
			return;
		}
	}

	private class AsyncPublisher implements Runnable {

		private String topic;
		private byte[] payload;
		private int qos;
		private boolean retained;

		// Retry params
		private boolean retry = false;
		private long retryDelay;
		private int numAttempts;

		public AsyncPublisher(String topic, byte[] payload, int qos, boolean retained, boolean retry, long retryDelay,
				int numAttempts) {
			this.topic = topic;
			this.payload = payload;
			this.qos = qos;
			this.retained = retained;
			this.retry = retry;
			this.retryDelay = retryDelay;
			this.numAttempts = numAttempts;
		}

		@Override
		public void run() {
			try {
				if (retry) {
					for (int i = 0; i < numAttempts; i++) {
						if (client == null || !client.isConnected()) {
							Thread.sleep(retryDelay);
						} else {
							handlePublish();
						}
					}

					logger.error("{}: Failed to publish message on {} after {} attempts", getClientId(), topic,
							numAttempts);
					throw new TahuException(TahuErrorCode.INTERNAL_ERROR,
							"Failed to publish message on " + topic + " after " + numAttempts + " attempts");
				} else {
					if (client == null) {
						throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "MQTT client is null");
					} else if (client.isConnected()) {
						handlePublish();
					} else {
						throw new TahuException(TahuErrorCode.INTERNAL_ERROR, "MQTT client not connected");
					}
				}
			} catch (Exception e) {
				logger.error("{}: Failed to publish", getClientId(), e);
			}
		}

		private void handlePublish() throws Exception {
			try {
				// Same path as publish() so async publishes cannot jump ahead of anything already buffered
				publishOrBuffer(topic, payload, qos, retained);
			} catch (TahuException e) {
				// Swallowed rather than rethrown so the retry loop in run() keeps its existing behavior
				logger.error("{}: Failed to publish on {} - {}", getClientId(), topic, e.getMessage());
			}
		}
	}

	private void shutdownConnectionMonitorThread() {
		if (connectionMonitorThread == null) {
			logger.debug("{}: Not shutting down ConnectionMonitorThread - its null", getClientId());
			return;
		}
		if (connectionMonitorThread.isAlive()) {
			logger.debug("{}: Shutting down ConnectionMonitorThread", getClientId());
			connectionMonitorThread.shutdown();
			connectionMonitorThread = null;
		} else {
			logger.debug("{}: Not shutting down ConnectionMonitorThread - its not alive", getClientId());
		}
	}

	private class ConnectionMonitorThread extends Thread {
		private ConnectionMonitor connectionMonitor;

		public ConnectionMonitorThread(ConnectionMonitor connectionMonitor) {
			super(connectionMonitor);
			this.connectionMonitor = connectionMonitor;
		}

		public void shutdown() {
			connectionMonitor.setKeepRunning(false);
			this.interrupt();
		}
	}

	private class ConnectionMonitor implements Runnable {

		private final TahuMqttAsyncClient monitoredClient;
		private final MqttClientId monitoredClientId;
		private boolean keepRunning = true;

		public ConnectionMonitor(TahuMqttAsyncClient client, MqttClientId clientId) {
			this.monitoredClient = client;
			this.monitoredClientId = clientId;
		}

		public void setKeepRunning(boolean keepRunning) {
			this.keepRunning = keepRunning;
		}

		public void run() {
			try {
				int connectionLostCounter = 0;
				while (keepRunning) {
					synchronized (clientLock) {
						if (monitoredClient != null) {
							if (!monitoredClient.isConnected()) {
								if (state.inProgress()) {
									logger.debug("{}: ConnectionMonitor - Attempting to connect", monitoredClientId);
									connectionLostCounter = 0;
								} else {
									logger.debug("{}: ConnectionMonitor - Not connected, incrementing counter",
											monitoredClientId);
									connectionLostCounter++;
								}
							} else {
								logger.trace("{}: ConnectionMonitor - Already connected", monitoredClientId);
								connectionLostCounter = 0;
							}
						} else {
							logger.debug("{}: ConnectionMonitor - Client is null - Uncaught connectionLost",
									getClientId());
							connectionLostCounter = 5;
						}
					}

					if (connectionLostCounter == 5 && callback != null) {
						callback.connectionLost(mqttServerName, mqttServerUrl, monitoredClientId,
								new Throwable(monitoredClientId + ": Uncaught paho disconnect"));
					}

					try {
						Thread.sleep(DEFAULT_CONNECT_MONITOR_INTERVAL);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						logger.debug("{}: ConnectionMonitor interrupted", monitoredClientId);
					}
				}
			} catch (Exception e) {
				logger.error("{}: ConnectionMonitor failed to keep running", monitoredClientId, e);
			}
		}
	}

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {

		// Check if we are in the process of disconnecting
		if (disconnectInProgress) {
			logger.warn("{}: Ignoring connect complete to {}, disconnect in progress", getClientId(), serverURI);
			// This potentially prevents a deadlock situation upon synchronizing on the clientLock below if a disconnect
			// is in progress and waiting on the client.disconnect() call
			return;
		}

		synchronized (clientLock) {
			if (reconnect) {
				logger.debug("{}: SUCCESSFULLY RECONNECTED to {}", getClientId(), getMqttServerUrl());
			}

			if (autoReconnect) {
				if (connectionMonitorThread == null || !connectionMonitorThread.isAlive()) {
					connectionMonitorThread = new ConnectionMonitorThread(new ConnectionMonitor(client, getClientId()));
					connectionMonitorThread.start();
				}
			}

			// The client is connected - renew online date, renew the connect time, clear disconnect time
			this.renewOnlineDate();
			this.renewConnectTime();
			this.clearDisconnectTime();

			logger.info("{}: Connected to {}", getClientId(), getMqttServerUrl());

			// Call connectComplete() with the callback
			getCallback().connectComplete(reconnect, getMqttServerName(), getMqttServerUrl(), getClientId());

			// Subscribe (or re-subscribe)
			if (!subscriptions.isEmpty()) {
				// Build up the arrays of topics and QoS levels
				int totalCount = subscriptions.size();
				int subscribedCount = 0;
				ArrayList<String> topicsList = new ArrayList<String>(subscriptions.keySet());

				while (subscribedCount < totalCount) {
					int topicsRemaining = totalCount - subscribedCount;
					// Don't attempt to publish more that the max topics per subscribe
					int size = topicsRemaining > maxTopicsPerSubscribe ? maxTopicsPerSubscribe : topicsRemaining;

					String[] topics = new String[size];
					int[] qosLevels = new int[size];

					for (int i = 0; i < size; i++) {
						String topic = topicsList.get(i + subscribedCount);
						topics[i] = topic;
						qosLevels[i] = subscriptions.get(topic);
					}

					String topicStr = Arrays.toString(topics);
					String qosStr = Arrays.toString(qosLevels);
					logger.debug("{}: server {} - Attempting to subscribe on topic {} with QoS={}", getClientId(),
							getMqttServerName(), topicStr, qosStr);
					try {
						client.subscribe(topics, qosLevels, null, new IMqttActionListener() {
							@Override
							public void onSuccess(IMqttToken asyncActionToken) {
								int[] grantedQos = asyncActionToken.getGrantedQos();
								if (Arrays.equals(qosLevels, grantedQos)) {
									logger.debug("{}: server {} - Successfully subscribed on {} on QoS={}",
											getClientId(), getMqttServerName(), topicStr, qosStr);
								} else {
									try {
										String grantedQosStr = Arrays.toString(grantedQos);
										logger.error("{}: server {} - Failed subscribe on {} granted QoS {} != {}",
												getClientId(), getMqttServerName(), topicStr, qosStr, grantedQosStr);

										// FIXME - remove This sleep is necessary due to:
										// https://github.com/eclipse/paho.mqtt.java/issues/850
										Thread.sleep(1000);

										synchronized (clientLock) {
											if (client != null) {
												// Force the disconnect and return
												client.disconnectForcibly(0, 1, false);
											}
										}
										return;
									} catch (Exception e) {
										logger.error(
												"{}: server {} - Failed disconnect on failed subscribe granted QoS",
												getClientId(), getMqttServerName(), e);
									}
								}
							}

							@Override
							public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
								synchronized (clientLock) {
									try {
										if (client != null) {
											logger.error(
													"{}: server {} - Failed to subscribe on {} - forcing disconnect",
													getClientId(), getMqttServerName(), topicStr);
											client.disconnectForcibly(0, 1, false);
										} else {
											logger.error("{}: server {} - Failed to subscribe on {} - client is null",
													getClientId(), getMqttServerName(), topicStr);
										}
									} catch (MqttException e) {
										logger.error("{}: server {} - Failed disconnect on failed subscribe",
												getClientId(), getMqttServerName(), e);
									}
								}
							}

						});
					} catch (MqttException e) {
						logger.error("{}: server {} - Failed to subscribe on {} with QoS={}", getClientId(),
								getMqttServerName(), topicStr, qosStr, e);
						break;
					}

					subscribedCount += size;

				}
			} else {
				if (trackFirstConnection && !firstConnection) {
					logger.warn("{}: No subscriptions for {}", getClientId(), getClientId());
				}
			}

			// Mark that the client has finished re-subscribing
			resubscribed = true;

			// Publish a standard Birth/Death Certificate if a baseTopic has been defined.
			if (onlineState) {
				publishBirthMessage();
			} else {
				try {
					this.publishLwt(true);
				} catch (Exception e) {
					logger.error("Failed to publish the LWT", e);
				}
			}

			firstConnection = false;
		}
	}

	/**
	 * Sets the 'track first connection' flag
	 * 
	 * @param trackFirstConnection - the 'track first connection' flag as {@link boolean}
	 */
	public void setTrackFirstConnection(boolean trackFirstConnection) {
		synchronized (clientLock) {
			this.trackFirstConnection = trackFirstConnection;
		}
	}

	public void setOnlineState(boolean newOnlineState) {
		synchronized (clientLock) {
			if (this.onlineState == newOnlineState) {
				return;
			} else {
				this.onlineState = newOnlineState;

				if (onlineState) {
					publishBirthMessage();
				} else {
					try {
						this.publishLwt(true);
					} catch (Exception e) {
						logger.error("Failed to publish the LWT when setting the online state", e);
					}
				}
			}
		}
	}

	public void publishBirthMessage() {
		synchronized (clientLock) {
			if (birthTopic != null && client.isConnected()) {
				try {

					if (useSparkplugStatePayload) {
						byte[] payload;
						try {
							ObjectMapper mapper = new ObjectMapper();
							StatePayload statePayload = new StatePayload(true, lastStateDeathPayloadTimestamp);
							logger.debug("{}: Publishing Sparkplug BIRTH on {} with retain={} and payload: {}",
									getClientId(), birthTopic, birthRetain, statePayload);
							payload = mapper.writeValueAsString(statePayload).getBytes();
						} catch (Exception e) {
							// Reconnecting cannot fix a payload that will not encode, so there is nothing to recover
							logger.error("{}: Failed to encode the BIRTH message on {}", getClientId(), birthTopic, e);
							return;
						}

						/*
						 * Deliberately outside the encode handler above - a failed publish must reach the recovery
						 * below rather than being logged and swallowed.
						 */
						publish(birthTopic, payload, MqttOperatorDefs.QOS1, birthRetain);
					} else {
						logger.debug("{}: Publishing BIRTH on {} with retain={}", getClientId(), birthTopic,
								birthRetain);
						publish(birthTopic, birthPayload, MqttOperatorDefs.QOS1, birthRetain);
					}
				} catch (TahuException ce) {
					logger.error("{}: Error in birth topic publish on connect", getClientId(), ce);
					try {
						client.disconnectForcibly(0, 1, false);
					} catch (Exception e) {
						logger.error("{}: Failed to disconnect after failed BIRTH publish", getClientId(), e);
					}
				}
			}
		}
	}

	/**
	 * Publishes the LWT at its configured QoS, falling back to QoS 0 if the acknowledged path cannot take it.
	 *
	 * IMM-5460. The LWT is the last thing this client sends, the publish buffer is torn down moments later, and on a
	 * clean DISCONNECT the MQTT server suppresses the Will - so the explicit publish is the only death certificate
	 * there will be. A buffered or rejected LWT is therefore as good as lost, where for ordinary data either outcome
	 * is recoverable.
	 *
	 * The backpressure check happens BEFORE publishing, not after. {@link #publishOrBuffer(String, byte[], int,
	 * boolean)} appends to the buffer and then returns null, so reacting to the null afterwards would leave a queued
	 * copy behind and could deliver the LWT twice - once at QoS 0 here and once from the drain thread. Testing the
	 * two conditions that cause buffering up front avoids that.
	 *
	 * A genuine failure - not connected, no client - is still rethrown so it reaches disconnect(), which is what 3.x
	 * intended by moving the publish outside the encode handler.
	 */
	private IMqttDeliveryToken publishLwtWithFallback(byte[] payload) throws TahuException {
		// The two conditions publishOrBuffer() buffers on: an exhausted in-flight window, or a non-empty buffer
		// that this message would have to queue behind to keep ordering.
		if (lwtQoS > MqttOperatorDefs.QOS0 && (getAvailablePublishPermits() == 0 || getPublishBufferDepth() > 0)) {
			return publishLwtAtQos0(payload);
		}

		IMqttDeliveryToken token;
		try {
			token = publish(lwtTopic, payload, lwtQoS, lwtRetain);
		} catch (TahuException e) {
			token = publishLwtAtQos0(payload);
			if (token == null) {
				throw e;
			}
			return token;
		}

		// Belt and braces for the race the check above cannot close: a permit can be taken, or the buffer filled,
		// between the check and the publish. Rare, and a duplicate retained death certificate is harmless.
		return token != null ? token : publishLwtAtQos0(payload);
	}

	/*
	 * Republishes the LWT at QoS 0. QoS 0 needs no in-flight permit and is never buffered, so it can still leave the
	 * process when the acknowledged path cannot. The acknowledgement is given up in exchange, which is the right
	 * trade for a retained message - a subscriber that connects later still reads it from the MQTT server.
	 */
	private IMqttDeliveryToken publishLwtAtQos0(byte[] payload) {
		logger.warn("{}: LWT on {} cannot be published at QoS {} - the in-flight window is exhausted or the publish "
				+ "buffer is in use. Retrying at QoS 0 so the death certificate is not lost.", getClientId(), lwtTopic,
				lwtQoS);
		try {
			return publish(lwtTopic, payload, MqttOperatorDefs.QOS0, lwtRetain);
		} catch (Exception e) {
			logger.error("{}: Failed to publish the LWT on {} at QoS 0", getClientId(), lwtTopic, e);
			return null;
		}
	}

	public void publishLwt(boolean waitForLwt) throws MqttException, TahuException {
		synchronized (clientLock) {
			boolean clientConnected = client != null && client.isConnected();
			boolean lwtDeliveryComplete = false;
			// Nothing attempted yet means nothing for disconnect() to escalate over
			lwtPublishSucceeded = true;
			if (lwtTopic != null && clientConnected) {
				boolean lwtPublished = false;
				// Flipped back to true below only if the message actually reaches the MQTT client
				lwtPublishSucceeded = false;
				synchronized (lwtDeliveryLock) {
					/*
					 * Synchronization with the deliveryComplete() callback is needed to ensure that
					 * the publish() call is fully completed and the lwtDeliveryToken is set before
					 * it is being nullified in the Paho callback.
					*/
					if (useSparkplugStatePayload) {
						byte[] payload;
						try {
							ObjectMapper mapper = new ObjectMapper();
							StatePayload statePayload = new StatePayload(false, lastStateDeathPayloadTimestamp);
							logger.debug("{}: Publishing Sparkplug LWT on {} with qos={} and retain={} and payload: {}",
									getClientId(), lwtTopic, lwtQoS, lwtRetain, statePayload);
							payload = mapper.writeValueAsString(statePayload).getBytes();
						} catch (Exception e) {
							// Reconnecting cannot fix a payload that will not encode, so there is nothing to recover
							logger.error("{}: Failed to encode the LWT message on {}", getClientId(), lwtTopic, e);
							return;
						}

						/*
						 * Deliberately outside the encode handler above so a failed publish reaches the caller rather
						 * than being logged and swallowed.
						 */
						lwtDeliveryToken = publishLwtWithFallback(payload);
					} else {
						logger.debug("{}: Publishing LWT on {} with qos={} and retain={}", getClientId(), lwtTopic,
								lwtQoS, lwtRetain);
						lwtDeliveryToken = publishLwtWithFallback(lwtPayload);
					}
					if (lwtDeliveryToken != null) {
						logger.debug("{}: published on LWT Topic={}, messageId={}", getClientId(), lwtTopic,
								lwtDeliveryToken.getMessageId());
						lwtPublished = true;
						lwtPublishSucceeded = true;
					} else {
						logger.warn("Failed to publish LWT {}", lwtTopic);
					}
				}

				if (lwtPublished && waitForLwt) {
					lwtDeliveryComplete = isLwtDeliveryComplete();
					logger.trace("{}: Completed LWT Delivery? {}", getClientId(), lwtDeliveryComplete);
				} else {
					logger.trace("{}: Not waiting for LWT", getClientId());
				}
			} else {
				logger.debug("{}: Not publishing LWT, client connected state: {}", getClientId(), clientConnected);
			}
		}
	}

	private Date getConnectTime() {
		return this.connectTime;
	}

	private Date getDisconnectTime() {
		return this.disconnectTime;
	}

	private void clearConnectTime() {
		this.connectTime = null;
	}

	private void clearDisconnectTime() {
		this.disconnectTime = null;
	}

	private void renewConnectTime() {
		this.connectTime = new Date();
	}

	private void renewDisconnectTime() {
		this.disconnectTime = new Date();
	}

	private long getConnectRetryInterval() {
		return connectRetryInterval;
	}

	public void setConnectRetryInterval(long connectRetryInterval) {
		this.connectRetryInterval = connectRetryInterval;
	}

	private long getConnectAttemptTimeout() {
		return connectAttemptTimeout;
	}

	public void setConnectAttemptTimeout(long connectAttemptTimeout) {
		this.connectAttemptTimeout = connectAttemptTimeout;
	}

	public boolean isAttemptingConnect() {
		return state.inProgress();
	}

	private String getErrorMessage(String prefix, Throwable throwable) {
		return new StringBuilder(prefix).append(": ").append(getErrorMessage(throwable)).toString();
	}

	private String getErrorMessage(Throwable throwable) {
		StringBuilder sb = new StringBuilder(throwable.getMessage());
		Throwable cause = throwable.getCause();
		if (cause != null) {
			sb.append(": ").append(getErrorMessage(cause));
		}
		return sb.toString();
	}

	private void logException(String message, Throwable throwable) {
		String errorMessage = getErrorMessage(message, throwable);
		if (logger.isTraceEnabled()) {
			// Only log the stack trace if trace is enabled
			logger.error("{}: {}", getClientId(), errorMessage, throwable);
		} else {
			logger.error("{}: {}", getClientId(), errorMessage);
		}
	}

	/*
	 * This method waits to ensure that the LWT gets published before graceful disconnect.
	 * It uses the 'keepAlive' to timeout if the lwtDeliveryToken is not cleared by the deliveryComplete() 
	 * Paho callback.
	 */
	private boolean isLwtDeliveryComplete() {
		int counter = keepAlive * 4;
		for (int i = 0; i < counter; i++) {
			try {
				if (lwtDeliveryToken == null) {
					logger.info("{}: LWT delivery confirmation - done waiting", getClientId());
					return true;
				} else {
					Thread.sleep(250);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.warn("{}: Interrupted while waiting for LWT", getClientId());
			}
		}
		lwtDeliveryToken = null;
		logger.warn("{}: LWT delivery confirmation - timeout", getClientId());
		return false;
	}
}
