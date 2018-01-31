package com.zhiyou100.groupingcomparator07;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RateHeroGourpingComparator extends WritableComparator {

	// 必须重写默认无参的构造方法，设置一下 key 的类型
	public RateHeroGourpingComparator() {
		
		// 注册 key 的类型
		super(Text.class, true);
	}

	// GourpingComparator 用于在 reduce 上进行 value 的聚合
	// 把相等的两个 key 的 value 进行聚合
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		// a 和 b 是 reduce 上的两个 key
		// 如果 a == b，聚合他们的 value
		Text aKey = (Text)a;
		Text bKey = (Text)b;
		
		// 分别取小数点后两位进行比较
		String aKeyValue = aKey.toString().substring(0, 4);
		String bKeyValue = bKey.toString().substring(0, 4);
		
		
		return -aKeyValue.compareTo(bKeyValue);
	}
}
