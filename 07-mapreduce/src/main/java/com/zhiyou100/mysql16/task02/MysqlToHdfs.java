package com.zhiyou100.mysql16.task02;

import java.io.IOException;

import javax.xml.transform.OutputKeys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MysqlToHdfs {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//设置远程连接的数据库
		DBConfiguration.configureDB(conf,
				"com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/hadoop_mapreduce_001",
				"master", "123456");
		Job job =Job.getInstance(conf, "MysqlToHdfs");
		job.setJarByClass(MysqlToHdfs.class);
		job.setMapperClass(MysqlToHdfsMapper.class);
		job.setReducerClass(MysqlToHdfsReducer.class);
		//使用类从数据库拿出来
		job.setOutputKeyClass(Department.class);
		job.setOutputValueClass(NullWritable.class);
		//设置输入格式
		job.setInputFormatClass(DBInputFormat.class);
		DBInputFormat.setInput(job, Department.class, "employee", "department!=2", "age desc", "id","name","age","department");
		//job.setOutputFormatClass(DBOutputFormat.class);
		
		Path outputPath =new Path("/mysql-hdfs");
		FileSystem.get(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//job.addFileToClassPath(new Path("/mysql-connector-java-5.1.45.jar"));
		
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag? "成功":"失败");
		
	}
	public static class MysqlToHdfsMapper extends Mapper<LongWritable, Department, Department, NullWritable>{
		@Override
		protected void map(LongWritable key, Department value, Mapper<LongWritable, Department, Department, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//读出的数据 直接传到reduce
			context.write(value, NullWritable.get());
		}
	}
	public static class MysqlToHdfsReducer extends Reducer<Department, NullWritable, Department, NullWritable>{
		@Override
		protected void reduce(Department key, Iterable<NullWritable> value,
				Reducer<Department, NullWritable, Department, NullWritable>.Context content) throws IOException, InterruptedException {
			//也是直接写出去
			content.write(key, NullWritable.get());
		}
	}
}
