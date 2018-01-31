package com.zhiyou100.secondarysort12;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class WordCount3SortComparator extends WritableComparator {
//reduce端进行排序
	public WordCount3SortComparator() {
		
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		Text aText = (Text)a;
		Text bText = (Text)b;
		
		String[] words1 = aText.toString().split("---");
		String[] words2 = bText.toString().split("---");

		String word1 = words1[0];
		String word2 = words2[0];
		int count1 = Integer.parseInt(words1[1]);
		int count2 = Integer.parseInt(words2[1]);
		
		// 字符串是按照字典序进行比较的
		// 11 < 2    23445 < 3
		
		if (word1.equals(word2)) {
			
			return count2 - count1;
		}else {
			
			// 1 3  = -1
			// 3 1  = 1
			return word2.compareTo(word1);
		}
	}
}
