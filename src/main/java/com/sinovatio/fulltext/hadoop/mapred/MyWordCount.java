package com.sinovatio.fulltext.hadoop.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration() ;
		
		//1.set job class
		Job job = new Job(conf, "wc") ;
		job.setJarByClass(MyWordCount.class);
		
		//2.set mapper and reducer
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		//3.set input folder and output folder
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//4.output the key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//5.submit job
		boolean success = job.waitForCompletion(true) ;
		System.out.println("success " + success);
		
		//6.finish
		System.exit(success ? 0 : 1);
		

	}
	
	/**
	 * mapper implements
	 * @author darwin
	 *
	 */
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		private Text keyWord = new Text() ;
		private static final IntWritable ONE = new IntWritable(1) ; 
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString() ;
			StringTokenizer tokenizer = new StringTokenizer(line) ;
			String word ;
			while(tokenizer.hasMoreTokens()) {
				word = tokenizer.nextToken() ;
				keyWord.set(word);
				context.write(keyWord, ONE);
			}
		}
	}
	
	/**
	 * reducer implements
	 * @author darwin
	 *
	 */
	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable() ;
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0 ;
			for(IntWritable value : values) {
				sum += value.get() ;
			}
			result.set(sum);
			context.write(key, result);
		}
	}

}
