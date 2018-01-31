package com.zhiyou100.combin09;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class CombinFile {

	/*
	 * 使用 MapReduce 合并 game.log 和 rate.log
	 * 合并后的格式：
	 *     文件名 + tab + 文件中某一行的内容（以 , 分隔）
	 *     game.log	xxxxx xxx xxx
	 *     game.log	xxxx xxx xxx
	 *     rate.log	xxx xxx
	 */

	public static void main(String[] args) {

		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job = Job.getInstance(conf, "CombinFile");
			job.setJarByClass(CombinFile.class);
			
			job.setMapperClass(CombinFileMapper.class);
			job.setReducerClass(CombinFileReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/game.log"));
			FileInputFormat.addInputPath(job, new Path("/rate.log"));
			FileInputFormat.addInputPath(job, new Path("/Subversion license.txt"));
			FileInputFormat.addInputPath(job, new Path("/TortoiseSVN License.txt"));
			//设置文件输出格式为sequence,文件保存也打开也需要以指定文件格式
			//为sequence
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			Path outputDir = new Path("/combin-file");
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

	public static class CombinFileMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 获取文件名
			// setup 只会执行一次，map 会执行 n 多次
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			Path filePath = inputSplit.getPath();
			
			String path = filePath.toString();
			int index = path.lastIndexOf("/");
			String name = path.substring(index + 1);
			
			outputKey.set(name);
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			context.write(outputKey, value);
		}
	}
	
	public static class CombinFileReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			for (Text text : value) {
				
				if (!text.toString().equals("")) {
					//去除空行
					context.write(key, text);
				}
			}
		}
	}
}
