package com.zhiyou100.secondarysort12;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount2 {

	public static void main(String[] args) {
		
		try {
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "wordcount2");
			job.setJarByClass(WordCount2.class);
			
			job.setMapperClass(WordCount2Mapper.class);
			job.setReducerClass(WordCount2Reducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			Path inputPath = new Path("/svn");
			FileInputFormat.addInputPath(job, inputPath);
			
			Path outputDir = new Path("/svn-count");
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

	
	public static class WordCount2Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private Text outputKey = new Text();

		private String path = "";
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			
			Path filePath = fileSplit.getPath();
			//System.out.println("-------------------;"+filePath.toString());
			path = filePath.toString().substring(23);
			//System.out.println("+++++++++++++++++++++++"+path);
		}


		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			
			String line = value.toString();
			
			String[] words = line.split("\\s+");
			
			for (String word : words) {
				
				word = word.replaceAll("\\pP|\\pS|\\pN", "");
				
				if (!word.equals("")) {
					
					outputKey.set(word + "\t" + path);
					
					context.write(outputKey, NullWritable.get());
				}
			}
		}
	}
	
	public static class WordCount2Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		private Text outputKey = new Text();

		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			// 统计循环次数，即单词出现的次数
			int sum = 0;
			
			for (NullWritable one : value) {
				
				sum += 1;
			}
			
			outputKey.set(key.toString() + "---" + sum);
			
			context.write(outputKey, NullWritable.get());
		}
	}
}
