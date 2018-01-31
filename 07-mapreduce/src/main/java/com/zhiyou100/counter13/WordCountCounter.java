package com.zhiyou100.counter13;

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

public class WordCountCounter {

	public static enum COUNTER_WORD_COUNT{LINES, WORDS;};
	
	public static void main(String[] args) {
		
		try {
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "WordCountCounter");
			job.setJarByClass(WordCountCounter.class);
			
			job.setMapperClass(WordCountCounterMapper.class);
			job.setReducerClass(WordCountCounterReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			Path inputPath = new Path("/WutheringHeights");
			FileInputFormat.addInputPath(job, inputPath);
			
			Path outputDir = new Path("/WutheringHeights-Count");
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

	
	public static class WordCountCounterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private Text outputKey = new Text();

		private String path = "";
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			
			Path filePath = fileSplit.getPath();
			
			path = filePath.toString().substring(36);
		}


		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			// 让 LINES 计数器自增 1
			context.getCounter(COUNTER_WORD_COUNT.LINES).increment(1L);
			
			String line = value.toString();
			
			String[] words = line.split("\\s+");
			
			for (String word : words) {
				
				word = word.replaceAll("\\pP|\\pS|\\pN", "");
				
				if (!word.equals("")) {
					
					context.getCounter(COUNTER_WORD_COUNT.WORDS).increment(1L);
					
					outputKey.set(word + "\t" + path);
					
					context.write(outputKey, NullWritable.get());
				}
			}
		}
	}
	
	public static class WordCountCounterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
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
