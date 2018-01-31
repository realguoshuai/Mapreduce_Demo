package com.zhiyou100.chain05;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou100.combiner03.GameRate.GameRateMapper;
import com.zhiyou100.combiner03.GameRate.GameRateReducer;
import com.zhiyou100.combiner03.ResultWritable;

public class GameHeroRate {

	public static void main(String[] args) {

		try {
			
			Configuration conf = new Configuration();

			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job = Job.getInstance(conf, "GameHeroRate");
			job.setJarByClass(GameHeroRate.class);
			
			// 上一个的输出，是下一个的输入
			
			
			// 搜索关键字的统计
			// 1. map 单词的查分
			// 2. map 谓词过滤(排除：的，了，么 等等)
			// 3. map 敏感词过滤(天安门)、
			// 4. reduce 单词计数
			// 5. map 过滤(数量 > 50)
			
			// 向 job 中添加多个 map，按照顺序执行
			ChainMapper.addMapper(job, GameRateMapper.class, LongWritable.class, Text.class, Text.class, ResultWritable.class, conf);
			
			// 设置 reduce，只能有一个
			ChainReducer.setReducer(job, GameRateReducer.class, Text.class, ResultWritable.class, Text.class, DoubleWritable.class, conf);
			
			// 在 reduce 还可以在继续执行任意个 map，最终把 map 的输出给用户看
			ChainReducer.addMapper(job, GameHeroRateMapper.class, Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, conf);

			Path inputPath = new Path("/game.log");
			FileInputFormat.addInputPath(job, inputPath);
			
			Path outputDir = new Path("/game-hero-rate");
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
	
	public static class GameHeroRateMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		protected void map(Text key, DoubleWritable value,
				Mapper<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {

			if (value.get() > 0.5) {
				
				// 这里写出的就是最终的结果
				context.write(key, value);
			}
		}
	} 
}
