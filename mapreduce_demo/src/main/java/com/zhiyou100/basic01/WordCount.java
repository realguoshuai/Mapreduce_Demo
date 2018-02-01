package com.zhiyou100.basic01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "weibo");
			job.setJarByClass(WordCount.class);
			
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			Path inputPath = new Path("hdfs://master:9000/weibo.txt");
			
			FileInputFormat.addInputPath(job, inputPath);
			
			Path outputDir = new Path("hdfs://master:9000/weibo");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);

			boolean flag = job.waitForCompletion(true);
			
			System.out.println(flag ? "成功":"失败");
		} catch (IOException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			
			e.printStackTrace();
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
	}

	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outputKey = new Text();
		
		private  Text outputValue = new Text();
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String row = value.toString();
			////每一行单词
			System.out.println(row+"+++++++++++++++++++++++");
			//String[] words = row.split("\\s+|:|,");
			String[] words = row.split(":");
			
			outputKey.set(words[0]);
			outputValue.set(words[1]);
			context.write(outputKey, outputValue);
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, Text, Text, Text> {
		
	
		private  Text outputValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			
			StringBuffer sBuffer =new StringBuffer();
			StringBuffer aBuffer =new StringBuffer();
			aBuffer.append("用户:");
			aBuffer.append(key);
			System.out.println();
			for (Text text : value) {
				sBuffer.append("粉丝:");
				sBuffer.append(text);
				sBuffer.append("\t");
			}
			key.set(aBuffer.toString());
			outputValue.set(sBuffer.toString());
			
			context.write(key,outputValue);
		}
	}
}





