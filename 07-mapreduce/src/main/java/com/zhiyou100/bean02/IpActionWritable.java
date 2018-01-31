package com.zhiyou100.bean02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IpActionWritable implements WritableComparable<IpActionWritable> {

	private String ip;

	private String action;

	public IpActionWritable() {
		super();
	}

	public IpActionWritable(String ip, String action) {
		super();
		this.ip = ip;
		this.action = action;
	}


	@Override
	public void write(DataOutput out) throws IOException {

		// 序列化-->对象到二进制
		// 使用 writeXXX() 方法把每一个属性转换为二进制 
		out.writeUTF(ip);
		out.writeUTF(action);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		// 反序列化-->二进制到对象
		// 使用 readXXX() 方法把每一个二进制转换为属性
		// 读取顺序要和书写顺序保持一致
		ip = in.readUTF();
		action = in.readUTF();
	}

	@Override
	public int compareTo(IpActionWritable o) {
		// 比较调用方法的对象和参数 o 之间的大小关系
		// 负数：小于
		// 0 ：等于
		// 正数：大于
		// 实际就是比较指定属性值的大小
		// eg：把 ip 的比较结果作为对象的比较结果
		return ip.compareTo(o.getIp());
	}

	@Override
	public String toString() {
		return ip + "++" + action;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}
}





