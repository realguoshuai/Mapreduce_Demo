package com.zhiyou100.mysql16.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;

public class DBMR {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//设置需要连接的数据库
		//使用具有远程登录权限的用户,程序是在集群运行,不是在master运行,需要远程访问数据库
		DBConfiguration.configureDB(conf,
				"com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/hadoop_mapreduce_001",
				"master", "123456");
		
		Job job = Job.getInstance(conf,"DBMR");
		
		job.setJarByClass(DBMR.class);
		job.setMapperClass(DBMRMapper.class);
		job.setReducerClass(DBMRReducer.class);
		
		job.setOutputKeyClass(User.class);
		job.setOutputValueClass(NullWritable.class);
		
		//设置输入格式为:数据库读取
		job.setInputFormatClass(DBInputFormat.class);
		//sql语句 可以自己拼
		//select fileNames.. from tableName where contation orderbBy
		DBInputFormat.setInput(job, User.class, "user", "age>10", "id desc", "id","name","age");
		job.setOutputFormatClass(DBOutputFormat.class);
		//insert into tableName(fileNames..)
		DBOutputFormat.setOutput(job, "user_bak", "id", "name", "age");
		
		//数据库操作必定要是使用驱动,把jar上传到hdfs上,做成缓存文件,供每一台机器使用
		//否则就需要把jar放在每一台机器的 hadoop 安装目录下的lib文件夹中
		job.addFileToClassPath(new Path("/mysql-connector-java-5.1.45.jar"));
		
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag? "成功":"失败");
		
	}
	
	
	public static class DBMRMapper extends Mapper<LongWritable, User, User, NullWritable>{
		@Override
		protected void map(LongWritable key, User value, Mapper<LongWritable, User, User, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//读出的数据 直接传到reduce
			context.write(value, NullWritable.get());
		}
	}
	public static class DBMRReducer extends Reducer<User, NullWritable, User, NullWritable>{
		@Override
		protected void reduce(User key, Iterable<NullWritable> value,
				Reducer<User, NullWritable, User, NullWritable>.Context content) throws IOException, InterruptedException {
			//也是直接写出去
			content.write(key, NullWritable.get());
		}
	}
}
