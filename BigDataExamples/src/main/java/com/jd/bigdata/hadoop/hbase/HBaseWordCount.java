package com.jd.bigdata.hadoop.hbase;

import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

import com.jd.bigdata.PropertiesHelper;

/**
 * An implementation of the Word Count example that will write the results to
 * HBase rather than HDFS
 * @author Jack
 *
 */
public class HBaseWordCount extends Configured implements Tool{


	/**
	 * Mapper. Handles one line of the file at a time. Splits each line into
	 * words and then outputs <word, 1> for each word
	 * 
	 * @author Jack
	 * 
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}

	}
	

	/**
	 * 
	 * @author Jack
	 *
	 */
	public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {
	
		public static final byte[] CF = "cf".getBytes();
		public static final byte[] COUNT = "count".getBytes();

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
	 	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    		int i = 0;
	    		for (IntWritable val : values) {
	    			i += val.get();
	    		}
	    		Put put = new Put(Bytes.toBytes(key.toString()));
	    		put.add(CF, COUNT, Integer.toString(i).getBytes());

	    		context.write(null, put);
	   	}
	}
	

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
	
		Properties properties = PropertiesHelper.loadProperties();
		
		HBaseConfiguration config = new HBaseConfiguration(super.getConf());
		config.set("hbase.master", properties.getProperty("hbase.master"));
		HBaseAdmin.checkHBaseAvailable(config);
		Job job = new Job(config, "ExampleWrite");
		job.setJarByClass(HBaseWordCount.class);     // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500);        
		scan.setCacheBlocks(false);        
		
		job.setMapperClass(MyMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		TableMapReduceUtil.initTableReducerJob(
				"test",        // output table
				MyTableReducer.class,    // reducer class
				job);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job!");
		}
		return 0;
	}
	
}
