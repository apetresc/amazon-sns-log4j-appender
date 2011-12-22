package com.twitsprout.appender.sns;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class SampleApp {
	
	static Logger logger = Logger.getLogger(SampleApp.class);

	public static void main(String[] args) throws IOException {
		Properties log4jProperties = new Properties();
		log4jProperties.load(SampleApp.class.getResourceAsStream("/log4j.properties.sample"));
		PropertyConfigurator.configure(log4jProperties);
		
		logger.fatal("This is a test");
		
		// This is currently necessary to do explicitly due to a "bug" in the AWS SDK.
		LogManager.shutdown();
	}
}
