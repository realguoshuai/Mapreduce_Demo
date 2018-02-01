package com.zhiyou100.topN14;

import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN01 {

	public static void main(String[] args) {

		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "wordcount");
			job.setJarByClass(TopN01.class);

			job.setMapperClass(TopN01Mapper.class);
			job.setReducerClass(TopN01Reducer.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(NullWritable.class);

			Path inputPath = new Path("/number.log");

			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/topN-01");
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


	public static class TopN01Mapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

		private IntWritable outputKey = new IntWritable();

		// 带排序的 set，默认是从小到大
		private TreeSet<Integer> treeSet = new TreeSet<>();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {

			Integer number = Integer.parseInt(value.toString());

			treeSet.add(number);

			if (treeSet.size() == 11) {

				// 超过 10 个就踢出最后一个
				treeSet.remove(treeSet.last());
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {

			for (Integer integer : treeSet) {

				outputKey.set(integer);

				context.write(outputKey, NullWritable.get());
			}
		}
	}

	public static class TopN01Reducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

		private IntWritable outputKey = new IntWritable();

		// 如果有多个 map 发送的就是 20 个了，还需要使用 treeSet 进行处理
		private TreeSet<Integer> treeSet = new TreeSet<>();
		
		@Override
		protected void reduce(IntWritable key, Iterable<NullWritable> value,
				Reducer<IntWritable, NullWritable, IntWritable, NullWritable>.Context context) throws IOException, InterruptedException {

			treeSet.add(key.get());
			
			if (treeSet.size() == 11) {
				
				treeSet.remove(treeSet.last());
			}
		}

		@Override
		protected void cleanup(Reducer<IntWritable, NullWritable, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {

			for (Integer integer : treeSet) {

				outputKey.set(integer);

				context.write(outputKey, NullWritable.get());
			}
		}
	}
}





