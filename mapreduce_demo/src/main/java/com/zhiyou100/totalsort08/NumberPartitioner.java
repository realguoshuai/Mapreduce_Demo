package com.zhiyou100.totalsort08;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NumberPartitioner extends Partitioner<IntWritable, NullWritable> {

	@Override
	public int getPartition(IntWritable key, NullWritable value, int numPartitions) {
		
		int number = key.get();
		
		if (number < 2000) {
			
			return 0;
		}else if (number < 4000) {
			
			return 1;
		}else if (number < 6000) {
			
			return 2;
		}else if (number < 8000) {
			
			return 3;
		}else {
			
			return 4;
		}
	}
}
