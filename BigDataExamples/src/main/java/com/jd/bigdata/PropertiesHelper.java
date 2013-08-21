package com.jd.bigdata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.jd.bigdata.flume.TwitterDataSource;

/**
 * Helper class to load properties from a properties file
 * @author Jack
 *
 */
public class PropertiesHelper {


	/**
	 * Loads the properties
	 * @return
	 * @throws IOException
	 */
	public static Properties loadProperties() throws IOException {
		InputStream in = TwitterDataSource.class
				.getResourceAsStream("/bigdata.properties");
		Properties properties = new Properties();
		properties.load(in);
		return properties;
	}
}
