package com.zhiyou100.groupingcomparator07;

import java.io.IOException;

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

public class RateHero {

	
	public static void main(String[] args) {

		try {
			
			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job = Job.getInstance(conf, "rate hero");
			job.setJarByClass(RateHero.class);
			
			job.setMapperClass(RateHeroMapper.class);
			job.setReducerClass(RateHeroReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			// 在 Reduce 时候，使用自定义的 key 聚合规则
			job.setGroupingComparatorClass(RateHeroGourpingComparator.class);

			// 在 Reduce 时候，使用自定义的 key 排序规则
			job.setSortComparatorClass(RateHeroGourpingComparator.class);
			
			Path inputPath = new Path("/rate.log");
			FileInputFormat.addInputPath(job, inputPath);
			
			Path outputDir = new Path("/rate-hero");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);
			
			boolean flag = job.waitForCompletion(false);
			
			System.exit(flag ? 0 : 1);
			
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}
	
	
	public static class RateHeroMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			
			String[] words = line.split("\\s");
			
			outputKey.set(words[1]);
			outputValue.set(words[0]);
			
			context.write(outputKey, outputValue);
		}
	}
	
	public static class RateHeroReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			StringBuffer names = new StringBuffer();
			
			for (Text name : value) {
				
				names.append(name);
				names.append(" ");
			}
			
			//0.55xxxxx
			String rate = key.toString().substring(0, 4);
			
			outputKey.set(rate);
			outputValue.set(names.toString());
			
			context.write(outputKey, outputValue);
		}
	}
}
