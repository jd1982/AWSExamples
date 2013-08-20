package com.jd.bigdata.hadoop.hbase;

import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.jd.bigdata.PropertiesHelper;


public class HBaseMapperExample extends Configured implements Tool{

	
	public HBaseMapperExample() throws Exception
	{
		
	}
	
//	public static class MyMapper extends TableMapper<Text, Text> {
//
//		  public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
//			  context.write(new Text("Hi"), new Text("Hi"));
//		   }
//		}
	
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
	
//	 public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
//
//			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//				int i = 0;
//				for (Text val : values) {
//					i += 1;
//				}
//				context.write(key, new IntWritable(i));
//			}
//		}
//	 
//	 
	
	public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {
		public static final byte[] CF = "cf".getBytes();
		public static final byte[] COUNT = "count".getBytes();

	 	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    		int i = 0;
	    		for (IntWritable val : values) {
	    			i += val.get();
	    		}
	    		Put put = new Put(Bytes.toBytes(key.toString()));
	    		put.add(CF, COUNT, Bytes.toBytes(i));

	    		context.write(null, put);
	   	}
	}
	
	
	public static void main(String args[]) throws Exception
	{
		Configuration c = new Configuration();
		ToolRunner.run(c, new HBaseMapperExample(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
	
		Properties properties = PropertiesHelper.loadProperties();
		
		HBaseConfiguration config = new HBaseConfiguration(super.getConf());
		config.set("hbase.master", properties.getProperty("hbase.master"));
		HBaseAdmin.checkHBaseAvailable(config);
		Job job = new Job(config, "ExampleWrite");
		job.setJarByClass(HBaseMapperExample.class);     // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		
		job.setMapperClass(MyMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		
//		TableMapReduceUtil.initTableMapperJob(
//		  "test",        // input HBase table name
//		  scan,             // Scan instance to control CF and attribute selection
//		  MyMapper.class,   // mapper
//		  Text.class,             // mapper output key
//		  Text.class,             // mapper output value
//		  job);

		TableMapReduceUtil.initTableReducerJob(
				"test",        // output table
				MyTableReducer.class,    // reducer class
				job);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println(args[0] + " " + args[1]);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job!");
		}
		return 0;
	}
	
}
