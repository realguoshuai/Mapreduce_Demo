package com.zhiyou100.mysql16.task;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.zhiyou100.entity.EmployeeResult;

public class HdfsToMysql {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		DBConfiguration.configureDB(conf,
				"com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/hadoop_mapreduce_001",
				"master", "123456");
		
		Job job =Job.getInstance(conf,"DBMR-TASK");
		job.setJarByClass(HdfsToMysql.class);
		job.setMapperClass(DBMRMapper.class);
		job.setReducerClass(DBMRReducer.class);
		
		job.setOutputKeyClass(Employee.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		//读取avro文件
		FileInputFormat.addInputPath(job, new Path("/avro-task-file/part-r-00000.avro"));
		job.setInputFormatClass(AvroKeyInputFormat.class);
		//设置序列化输入内容的模式(Schema)
		AvroJob.setInputKeySchema(job, EmployeeResult.getClassSchema());
		
		//输出到数据库
		DBOutputFormat.setOutput(job, "employee", "id", "name", "age","department");
		job.addFileToClassPath(new Path("/mysql-connector-java-5.1.45.jar"));
				
		boolean flag = job.waitForCompletion(true);
		System.out.println(flag? "成功":"失败");
	}

public static class DBMRMapper extends Mapper<AvroKey<EmployeeResult>, NullWritable, Employee, NullWritable> {
		
		private Employee outputKey = new Employee();
		
		@Override
		protected void map(AvroKey<EmployeeResult> key, NullWritable value, Mapper<AvroKey<EmployeeResult>, NullWritable, Employee, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//获取Avro序列化文件中的每一个对象
			EmployeeResult smallFile = key.datum();
			
			//统计每个部门出现的人数()
			String strings = smallFile.getContent().toString();
		
			int ids =strings.lastIndexOf("'content: '");
			String id = strings.substring(ids + 1);
			String[] split_id = id.split("\\s+");
			
			outputKey.setId(Integer.parseInt(split_id[0]));
			outputKey.setName(split_id[1]);
			outputKey.setAge(Integer.parseInt(split_id[2]));
			outputKey.setDepartment(Integer.parseInt(split_id[3]));
			
			context.write(outputKey, NullWritable.get());
		}
	}
	
	public static class DBMRReducer extends Reducer<Employee, NullWritable, Employee, NullWritable> {
		@Override
		protected void reduce(Employee key, Iterable<NullWritable> value,
				Reducer<Employee, NullWritable, Employee, NullWritable>.Context content)
				throws IOException, InterruptedException {
			// 也是直接写出去
			content.write(key, NullWritable.get());
		}
	}
}
