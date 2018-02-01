package com.zhiyou100.invertedindex10;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "InvertedIndex");
			job.setJarByClass(InvertedIndex.class);

			// 设置使用的 map 和 reduce 的类
			job.setMapperClass(InvertedIndexMapper.class);
			job.setReducerClass(InvertedIndexReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// part-r-00000 是 SequenceFile，只能用 SequenceFileInputFormat 来读取
			job.setInputFormatClass(SequenceFileInputFormat.class);
			
			Path inputPath = new Path("/combin-file/part-r-00000");
			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/inverted-index");
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

	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, Text> {

		private Text outputKey = new Text();

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			// key 是文件名，value 是文件中的每一行
			String line = value.toString();
			
			String[] words = line.split("\\s+|,");
			
			for (String word : words) {
				
				// 单词作为 key，文件名最为 value 输出
				outputKey.set(word);
				
				context.write(outputKey, key);
			}
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

		private Text outputValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			HashSet<String> set = new HashSet<>();
			
			for (Text name : value) {

				set.add(name.toString());
			}
			
			StringBuffer result = new StringBuffer();

			for (String name : set) {
				
				result.append(name);
				result.append("    ");
			}
			
			outputValue.set(result.toString());
			
			context.write(key, outputValue);
		}
	}
}
