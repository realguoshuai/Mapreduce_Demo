package com.zhiyou100.sort04;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;

import org.apache.hadoop.io.WritableComparable;

public class HeroWritable implements WritableComparable<HeroWritable> {

	private String name;
	private double rate;
	
	public HeroWritable() {
		super();
	}
	
	
	public HeroWritable(String name, double rate) {
		super();
		this.name = name;
		
		BigDecimal bd = new BigDecimal(rate);
		
		this.rate = bd.setScale(2, BigDecimal.ROUND_UP).doubleValue();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(name);
		out.writeDouble(rate);
	}


	@Override
	public void readFields(DataInput in) throws IOException {

		name = in.readUTF();
		rate = in.readDouble();
	}


	@Override
	public int compareTo(HeroWritable o) {
		
		if (rate > o.getRate()) {
			
			return 1;
		}else if (rate == o.getRate()) {
			
			return 0;
		}else {
			
			return -1;
		}
	}

	@Override
	public String toString() {
		return name + "===========" + rate;
	}


	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public double getRate() {
		return rate;
	}
	public void setRate(double rate) {
		this.rate = rate;
	}
}
