package com.zhiyou100.sort04;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.partitioner06.RatePartitioner;

public class HeroRate {

	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "GameRate");
			job.setJarByClass(HeroRate.class);
			
			job.setMapperClass(HeroRateMapper.class);
			job.setReducerClass(HeroReducer.class);
			
			job.setOutputKeyClass(HeroWritable.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setNumReduceTasks(3);
			job.setPartitionerClass(RatePartitioner.class);
			
			Path inputPath = new Path("/rate.log");
			FileInputFormat.addInputPath(job, inputPath);
			
			Path outputDir = new Path("/hero-rate");
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
	
	public static class HeroRateMapper extends Mapper<LongWritable, Text, HeroWritable, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, HeroWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			
			String[] words = line.split("\\s+");
			
			HeroWritable outputKey = new HeroWritable(words[0], Double.parseDouble(words[1]));
			
			context.write(outputKey, NullWritable.get());
		}
	}
	
	public static class HeroReducer extends Reducer<HeroWritable, NullWritable, HeroWritable, NullWritable> {

		@Override
		protected void reduce(HeroWritable key, Iterable<NullWritable> value,
				Reducer<HeroWritable, NullWritable, HeroWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			for (NullWritable nw : value) {
				
				context.write(key, nw);
			}
		}
	}
}
