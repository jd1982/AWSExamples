package com.jd.bigdata.hadoop.hbase;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.jd.bigdata.PropertiesHelper;


public class HBaseMapperExample extends Configured implements Tool{

	
	public HBaseMapperExample() throws Exception
	{
		
	}
	
	public static class MyMapper extends TableMapper<Text, Text> {

		  public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
		    // process data for the row from the Result instance.
		   }
		}
	
	 public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				int i = 0;
				for (IntWritable val : values) {
					i += val.get();
				}
				context.write(key, new IntWritable(i));
			}
		}
	
	
	
	public static void main(String args[]) throws Exception
	{
		Configuration c = new Configuration();
		ToolRunner.run(c, new HBaseMapperExample(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
	
		Properties properties = PropertiesHelper.loadProperties();
		
		HBaseConfiguration config = new HBaseConfiguration(super.getConf());
		config.set("hbase.master", properties.getProperty("hbase.master"));
		HBaseAdmin.checkHBaseAvailable(config);
		Job job = new Job(config, "ExampleRead");
		job.setJarByClass(HBaseMapperExample.class);     // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		
		TableMapReduceUtil.initTableMapperJob(
		  "test",        // input HBase table name
		  scan,             // Scan instance to control CF and attribute selection
		  MyMapper.class,   // mapper
		  Text.class,             // mapper output key
		  Text.class,             // mapper output value
		  job);
		job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper

		job.setReducerClass(MyReducer.class);
		
		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job!");
		}
		return 0;
	}
	
}
