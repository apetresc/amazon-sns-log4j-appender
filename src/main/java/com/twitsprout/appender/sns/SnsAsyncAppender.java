package com.twitsprout.appender.sns;

import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.PublishRequest;

public class SnsAsyncAppender extends AppenderSkeleton {
	private final AmazonSNSAsync sns;
	private String topicArn;
	
	private boolean snsClosed;

	public SnsAsyncAppender() {
		try {
			this.sns = new AmazonSNSAsyncClient(new PropertiesCredentials(
					SnsAsyncAppender.class.getResourceAsStream("/AwsCredentials.properties")));
			this.snsClosed = true;
		} catch (IOException ioe) {
			throw new RuntimeException("Could not instantiate SnsAsyncAppender", ioe);
		}
	}
	
	public SnsAsyncAppender(String topicName) {
		try {
			this.sns = new AmazonSNSAsyncClient(new PropertiesCredentials(
					SnsAsyncAppender.class.getResourceAsStream("/AwsCredentials.properties")));
			this.topicArn = sns.createTopic(new CreateTopicRequest(topicName)).getTopicArn();
			this.snsClosed = false;
		} catch (IOException ioe) {
			throw new RuntimeException("Could not instantiate SnsAsyncAppender", ioe);
		} catch (AmazonClientException ace) {
			throw new RuntimeException("Could not instantiate SnsAsyncAppender", ace);
		}
	}
	
	/* For unit-testing purposes */
	/* package */ SnsAsyncAppender(AmazonSNSAsync sns, String topicName) {
		this.sns = sns;
		this.topicArn = sns.createTopic(new CreateTopicRequest(topicName)).getTopicArn();
		this.snsClosed = false;
	}
	
	public void setTopicName(String topicName) {
		try {
			topicArn = sns.createTopic(new CreateTopicRequest(topicName)).getTopicArn();
			snsClosed = false;
		} catch (Exception e) {
			LogLog.error("Could not set topic name to: " + topicName);
			snsClosed = true;
		}
	}

	@Override
	protected void append(LoggingEvent event) {
		if (!checkEntryConditions()) {
			return;
		}

		String logMessage;
		if (layout != null) {
			logMessage = layout.format(event);
		} else {
			logMessage = event.getRenderedMessage();
		}
		if (logMessage.getBytes().length > 64 * 1024) {
			// SNS has a 64K limit on each published message.
			logMessage = new String(Arrays.copyOf(logMessage.getBytes(), 64 * 1024));
		}
		
		try {
			sns.publishAsync(new PublishRequest(
					topicArn,
					logMessage,
					event.getLoggerName() + " log: " + event.getLevel().toString()
					));
		} catch (AmazonClientException ase) {
			LogLog.error("Could not log to SNS", ase);
		}
	}
	
	protected boolean checkEntryConditions() {
		return !snsClosed;
	}

	public String getTopicArn() {
		return topicArn;
	}

	public void close() {
		if (snsClosed) return;
		snsClosed = true;
		sns.shutdown();
		((AmazonSNSAsyncClient) sns).getExecutorService().shutdownNow();
	}

	public boolean requiresLayout() {
		return false;
	}
		
}
