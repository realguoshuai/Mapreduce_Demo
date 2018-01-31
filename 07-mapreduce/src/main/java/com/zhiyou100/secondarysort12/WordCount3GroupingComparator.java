package com.zhiyou100.secondarysort12;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCount3GroupingComparator extends WritableComparator {
//reduce 端进行聚合   hello  A.TXT B.TXT
	public WordCount3GroupingComparator() {
		
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
//排序
		Text aText = (Text)a;
		Text bText = (Text)b;
		
		String word1 = aText.toString().split("---")[0];
		String word2 = bText.toString().split("---")[0];

		return word1.compareTo(word2);
	}
}
