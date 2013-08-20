package com.jd.bigdata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.jd.bigdata.flume.TwitterDataSource;

public class PropertiesHelper {


	public static Properties loadProperties() throws IOException {
		InputStream in = TwitterDataSource.class
				.getResourceAsStream("/bigdata.properties");
		Properties properties = new Properties();
		properties.load(in);
		return properties;
	}
}
