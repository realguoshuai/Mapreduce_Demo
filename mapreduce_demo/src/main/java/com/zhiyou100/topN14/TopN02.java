package com.zhiyou100.topN14;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN02 {

	public static void main(String[] args) {

		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "wordcount");
			job.setJarByClass(TopN02.class);

			job.setMapperClass(TopN02Mapper.class);
			job.setReducerClass(TopN02Reducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			Path inputPath = new Path("/WutheringHeights-Count/part-r-00000");

			FileInputFormat.addInputPath(job, inputPath);

			Path outputDir = new Path("/topN-02");
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


	public static class TopN02Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private Text outputKey = new Text();

		// 带排序的 set，默认是从小到大
		private TreeSet<String> treeSet = new TreeSet<>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {

				Integer n1 = Integer.parseInt(o1.split("---")[1]);
				Integer n2 = Integer.parseInt(o2.split("---")[1]);

				return n1.compareTo(n2);
			}
		});
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			treeSet.add(value.toString());

			if (treeSet.size() == 11) {

				// 超过 10 个就踢出最后一个
				treeSet.remove(treeSet.first());
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			for (String string : treeSet) {

				outputKey.set(string);

				context.write(outputKey, NullWritable.get());
			}
		}
	}

	public static class TopN02Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		private Text outputKey = new Text();

		// 如果有多个 map 发送的就是 20 个了，还需要使用 treeSet 进行处理
		private TreeSet<String> treeSet = new TreeSet<>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {

				Integer n1 = Integer.parseInt(o1.split("---")[1]);
				Integer n2 = Integer.parseInt(o2.split("---")[1]);

				return n2.compareTo(n1);
			}
			
		});
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {

			treeSet.add(key.toString());
			
			if (treeSet.size() == 11) {
				
				treeSet.remove(treeSet.last());
			}
		}

		@Override
		protected void cleanup(Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			for (String string : treeSet) {

				outputKey.set(string);

				context.write(outputKey, NullWritable.get());
			}
		}
	}
}





