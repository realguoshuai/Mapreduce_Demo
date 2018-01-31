package com.zhiyou100.combiner03;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GameRate {

	public static void main(String[] args) {
		
		try {
			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "GameRate");
			job.setJarByClass(GameRate.class);
			
			job.setMapperClass(GameRateMapper.class);
			job.setCombinerClass(GameRateCombiner.class);
			job.setReducerClass(GameRateReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(ResultWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			Path inputPath = new Path("/game.log");
			FileInputFormat.addInputPath(job, inputPath);
			
			Path outputDir = new Path("/game-rate");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);
			
			boolean flag = job.waitForCompletion(true);
			
			System.exit(flag ? 0 : 1);
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}

	public static class GameRateMapper extends Mapper<LongWritable, Text, Text, ResultWritable> {
		
		private Text outputKey = new Text();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, ResultWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split(",");
			
			outputKey.set(words[0]);
			ResultWritable outputValue = new ResultWritable(Integer.parseInt(words[1]), 1);
			
			context.write(outputKey, outputValue);
		}
	}
	
	public static class GameRateCombiner extends Reducer<Text, ResultWritable, Text, ResultWritable> {

		@Override
		protected void reduce(Text key, Iterable<ResultWritable> value,
				Reducer<Text, ResultWritable, Text, ResultWritable>.Context context)
				throws IOException, InterruptedException {
			
			int succ = 0;
			int count = 0;
			
			for (ResultWritable rs : value) {
				
				succ += rs.getSucc();
				count += rs.getCount();
			}
			
			context.write(key, new ResultWritable(succ, count));
		}
	}
	
	
	public static class GameRateReducer extends Reducer<Text, ResultWritable, Text, DoubleWritable> {
		
		private Text outputKey = new Text();
		private DoubleWritable outputValue = new DoubleWritable();
		
		@Override
		protected void reduce(Text key, Iterable<ResultWritable> value,
				Reducer<Text, ResultWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			
			int succ = 0;
			int count = 0;
			
			for (ResultWritable rw : value) {
				
				succ += rw.getSucc();
				count += rw.getCount();
			}
			
			outputKey = key;
			outputValue.set(succ * 1.0 / count);
			
			context.write(outputKey, outputValue);
		}
	}
}
