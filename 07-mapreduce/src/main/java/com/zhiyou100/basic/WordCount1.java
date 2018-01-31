package com.zhiyou100.basic;

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

public class WordCount1 {
	public static void main(String[] args) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "weibo");
			job.setJarByClass(WordCount1.class);

			job.setMapperClass(WordCount1Mapper.class);
			job.setReducerClass(WordCount1Reduce.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path("/weibo.txt"));
			Path outputDir = new Path("/weibo-1");
			FileSystem.get(conf).delete(outputDir,true);
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
	public static class WordCount1Mapper extends Mapper<LongWritable, Text, Text, Text>{
		/*
		 * 用户A:关注了B,C,D,F,E,O,不是A的粉丝是..
		 * */
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//分割两次
			String line = value.toString();
			String[] words=line.split(":");
            String user = words[0];
            //关注的每一个人
            String[] focus = words[1].split(",");
            outputValue.set(user);
            for (int i = 0; i < focus.length; i++) {
                outputKey.set(focus[i]);
                context.write(outputKey, outputValue);
            }
		}
	}
	public static class WordCount1Reduce extends Reducer<Text, Text, Text, Text>{
		private Text outputValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer focus = new StringBuffer();
			//StringBuffer user = new StringBuffer();
			for (Text fan : value) {
				//user.append("用户:");
				//user.append();
				//focus.append("粉丝:");
				focus.append(fan);
				focus.append(",");
				//System.out.println(fan+"------------------------");
			}
			outputValue.set(focus.toString());
			context.write(key, outputValue);
		}
		
	}
}

