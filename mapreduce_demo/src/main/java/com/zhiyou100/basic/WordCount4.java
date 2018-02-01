package com.zhiyou100.basic;

import java.io.IOException;
import java.util.Arrays;

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

public class WordCount4 {
	public static void main(String[] args) {
		/*求共同关注(不一定是共同好友)
		 * 需要引入的表,粉丝-->关注人  格式
		 */
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			Job job = Job.getInstance(conf, "weibo");
			job.setJarByClass(WordCount4.class);

			job.setMapperClass(WordCount4Mapper.class);
			job.setReducerClass(WordCount4Reduce.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path("/weibo.txt"));
			Path outputDir = new Path("/weibo-4");
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
	public static class WordCount4Mapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String user = line.split(":")[0];
			String fans = line.split(":")[1];
			
			String[] fan = fans.split(",");
			outputValue.set(user);
			//按照字典序(ASCII)的顺序进行排序
			Arrays.sort(fan);
			for (int i = 0; i < fan.length; i++) {
				for (int j = i+1; j < fan.length; j++) {
					outputKey.set(fan[i]+"-"+fan[j]+":");
					context.write(outputKey, outputValue);
				}
			}
		}
		
	}
	public static class WordCount4Reduce extends Reducer<Text, Text, Text, Text>{
		private Text outputValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer stringBuffer = new StringBuffer();
			for (Text text : value) {
				stringBuffer.append(text).append(",");
			}
			stringBuffer.deleteCharAt(stringBuffer.length()-1);
			outputValue.set(stringBuffer.toString());
			context.write(key, outputValue);
		}
	}
	
}

