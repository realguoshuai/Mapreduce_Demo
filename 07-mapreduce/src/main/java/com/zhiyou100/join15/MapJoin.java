package com.zhiyou100.join15;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

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

public class MapJoin {

	public static void main(String[] args) {

		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "Reduce Join");
			job.setJarByClass(MapJoin.class);

			job.setMapperClass(MapJoinMapper.class);
			job.setReducerClass(MapJoinReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			Path inputPath = new Path("/emp.txt");

			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/map-join");
			FileSystem.get(conf).delete(outputDir, true);
			FileOutputFormat.setOutputPath(job, outputDir);

			// 可以添加任意个
			job.addCacheFile(new Path("/dep.txt").toUri());
			
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


	public static class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private Text outputKey = new Text();
		
		private HashMap<String, String> smallTable = new HashMap<>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			// 获取缓存文件的路径，path 是绝对路径，需要减去前边的 \ 变成相对路径
			String path = context.getCacheFiles()[0].toString();

			// 使用 IO 流读取缓存文件的内容
			try (FileReader fileReader = new FileReader(path.substring(1));
					BufferedReader bufferedReader = new BufferedReader(fileReader);) {

				String content = null;

				while ((content = bufferedReader.readLine()) != null) {

					String[] words = content.split("\\s+");
					
					smallTable.put(words[0], words[1]);
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			String depId = line.split("\\s+")[2];
			
			outputKey.set(line + "\t" + smallTable.get(depId));
			
			context.write(outputKey, NullWritable.get());
		}
	}

	public static class MapJoinReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		
			context.write(key, NullWritable.get());
		}	
	}
}





