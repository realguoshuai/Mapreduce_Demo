package com.zhiyou100.partitioner06;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.zhiyou100.sort04.HeroWritable;

// 自定义分区，需要继承 Partitioner，实现 getPartition 方法，输出 reduce 编号
public class RatePartitioner extends Partitioner<HeroWritable, NullWritable> {

	@Override
	public int getPartition(HeroWritable key, NullWritable value, int numPartitions) {

		// 胜率 > 52% 放在 0 中
		// 胜率 > 48% 放在 1 中
		// 其他的放在 2 中
		
		if (key.getRate() > 0.52) {
			
			return 0 % numPartitions;
		}else if (key.getRate() > 0.48) {
			
			return 1 % numPartitions;
		}else {
			
			return 2 % numPartitions;
		}
	}

}
