package com.zhiyou100.jobcontrol11;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.zhiyou100.combin09.CombinFile;
import com.zhiyou100.combin09.CombinFile.CombinFileMapper;
import com.zhiyou100.combin09.CombinFile.CombinFileReducer;
import com.zhiyou100.invertedindex10.InvertedIndex;
import com.zhiyou100.invertedindex10.InvertedIndex.InvertedIndexMapper;
import com.zhiyou100.invertedindex10.InvertedIndex.InvertedIndexReducer;

public class ManyJobControl {

	public static void main(String[] args) {

		// 通过 jobcontrol 我们可以设置 job 之间的依赖
		// 让某个 job 在指定的 job 执行后再执行
		
		// 1. 列出具有依赖关系的所有任务
		// 2. 把普通任务转换为受控任务（ControlledJob）
		// 3. 设置依赖关系
		// 4. 创建任务控制器，并开始执行任务
		
		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job1 = Job.getInstance(conf, "CombinFile");
			job1.setJarByClass(CombinFile.class);
			
			job1.setMapperClass(CombinFileMapper.class);
			job1.setReducerClass(CombinFileReducer.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job1, new Path("/game.log"));
			FileInputFormat.addInputPath(job1, new Path("/rate.log"));
			FileInputFormat.addInputPath(job1, new Path("/Subversion license.txt"));
			FileInputFormat.addInputPath(job1, new Path("/TortoiseSVN License.txt"));

			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			Path job1OutputDir = new Path("/combin-file");
			FileSystem.get(conf).delete(job1OutputDir, true);
			FileOutputFormat.setOutputPath(job1, job1OutputDir);

						
			Job job2 = Job.getInstance(conf, "InvertedIndex");
			job2.setJarByClass(InvertedIndex.class);

			// 设置使用的 map 和 reduce 的类
			job2.setMapperClass(InvertedIndexMapper.class);
			job2.setReducerClass(InvertedIndexReducer.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			// part-r-00000 是 SequenceFile，只能用 SequenceFileInputFormat 来读取
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			Path job2InputPath = new Path("/combin-file/part-r-00000");
			FileInputFormat.addInputPath(job2, job2InputPath);

			Path job2OutputDir = new Path("/inverted-index");
			FileSystem.get(conf).delete(job2OutputDir, true);
			FileOutputFormat.setOutputPath(job2, job2OutputDir);
			
			
			// 把普通 job 转换为受控 job
			ControlledJob controlledJob1 = new ControlledJob(conf);
			controlledJob1.setJob(job1);
			
			ControlledJob controlledJob2 = new ControlledJob(conf);
			controlledJob2.setJob(job2);
			
			// 设置 job2 依赖于 job1
			// 只有依赖的 job 都执行完了，才能执行这个 job
			controlledJob2.addDependingJob(controlledJob1);
			
			// 创建任务控制器，为这一组任务起个名字
			JobControl jobControl = new JobControl("Combin Inverted");
			
			// 设置任务组里有那些任务
			jobControl.addJob(controlledJob1);
			jobControl.addJob(controlledJob2);
			
			// 开始执行任务组，JobControl 实现了 runable 接口
			// 需要使用线程执行
			Thread jobControlThread = new Thread(jobControl);
			jobControlThread.start();
			
			while(true) {
				if (jobControl.allFinished()) {
					
					System.out.println("任务全部完成了");
					
					jobControl.stop();
					
					break;
				}
				
				if (jobControl.getFailedJobList().size() > 0) {
					
					System.out.println("有任务失败了");
					
					jobControl.stop();
					
					break;
				}
				
				Thread.sleep(1000);
			}
		} catch (IOException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}
}
