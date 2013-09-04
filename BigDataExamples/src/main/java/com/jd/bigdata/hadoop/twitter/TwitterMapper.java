package com.jd.bigdata.hadoop.twitter;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

/**
 * Mapper. Handles one line of the file at a time. Splits each line into
 * words and then outputs <word, 1> for each word
 * 
 * @author Jack
 * 
 */
public class TwitterMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, LongWritable> {

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	public void map(LongWritable key, Text value,
			OutputCollector<Text, LongWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		
		try
		{
			Status status = DataObjectFactory.createStatus(line);
	
			
			output.collect(new Text(status.getUser().getScreenName()), new LongWritable(status.getRetweetCount()));
		}
		catch(TwitterException ex)
		{
			
		}
		
	}
}
