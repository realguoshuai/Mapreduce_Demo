package com.zhiyou100.basic;

import java.io.IOException;
import java.util.ArrayList;

import javax.ws.rs.core.NewCookie;
import javax.xml.transform.OutputKeys;

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


// 互粉
public class WordCount2 {

	public static void main(String[] args) throws Exception {
		// 指定hdfs相关的参数
		
		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "fans");
			job.setJarByClass(WordCount2.class);

			job.setMapperClass(WordCount2Mapper.class);
			job.setReducerClass(WordCount2Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			Path inputPath = new Path("/weibo.txt");

			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/weibo-2");
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

	private static class WordCount2Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text OutputKeys = new Text();
		private Text OutputValues = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(":");
			String person = words[0];
			String[] fens = words[1].split(",");
			OutputValues.set(person);
			//如果a的粉丝contains b && a的粉丝contains b-->输出 ab互粉
			for (String fen : fens) {
				//用户 -粉丝
				OutputKeys.set(combineStr(person, fen));
				context.write(OutputKeys,OutputValues);
				System.out.println(OutputKeys+"*******************************"+OutputValues.toString());
			}
			//a a的粉丝 , a
		}

		// 整合排序ab
		private String combineStr(String a, String b) {
			if (a.compareTo(b) > 0) {
				return b + "-" + a;
			} else {
				return a + "-" + b;
			}
		}
	}

	private static class WordCount2Reducer extends Reducer<Text, Text, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//a a的粉丝b ,a   自动聚合!!!  出现两次 就说明互粉了
			int sum = 0;
			for (Text text : values) {
				//key  所有结果 (用户-粉丝)value  用户 
				//ab = ba 存在两个
				sum++;
				System.out.println(text+"------------"+sum+"---------------------");
			} 
			if (sum == 2) {
				context.write(key, NullWritable.get());
			}
		}
	}
}
