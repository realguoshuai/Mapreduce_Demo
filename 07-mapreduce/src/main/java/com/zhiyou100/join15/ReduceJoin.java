package com.zhiyou100.join15;

import java.io.IOException;
import java.util.ArrayList;

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

public class ReduceJoin {

	public static void main(String[] args) {
		
		try {
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "Reduce Join");
			job.setJarByClass(ReduceJoin.class);
			
			job.setMapperClass(ReduceJoinMapper.class);
			job.setReducerClass(ReduceJoinReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			Path inputPath1 = new Path("/emp.txt");
			Path inputPath2 = new Path("/dep.txt");

			FileInputFormat.addInputPath(job, inputPath1);
			FileInputFormat.addInputPath(job, inputPath2);
			
			Path outputDir = new Path("/reduce-join");
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

	
	public static class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		private String fileName = "";
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			
			String path = fileSplit.getPath().toString();
			
			int index = path.lastIndexOf("/");
			
			fileName = path.substring(index + 1);
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			String[] words = line.split("\\s+");
			
			if (fileName.equals("emp.txt")) {
				
				outputKey.set(words[2]);
				outputValue.set("emp#" + words[0] + "\t" + words[1]);

				context.write(outputKey, outputValue);
			}else if (fileName.equals("dep.txt")) {
				
				outputKey.set(words[0]);
				outputValue.set("dep#" + words[1]);
				
				
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private Text outputkey = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			// 先找部门
			String depName = "";
			
			// 迭代器只会前进，不会后退，需要保存元素，进行第二次遍历
			ArrayList<String> strings = new ArrayList<String>();
			
			for (Text text : value) {
				
				String string = text.toString();
				
				if (string.startsWith("dep#")) {
					
					depName = string.substring(4);
				}else {
					
					strings.add(string);
				}
			}
			
			// 再找员工
			for (String string2 : strings) {
				
				if (string2.startsWith("emp#")) {
					
					// 拼接输出
					String empInfo = string2.substring(4);
					
					outputkey.set(empInfo + "\t" + depName);
					
					context.write(outputkey, NullWritable.get());
				}
			}
		}
	}
}





