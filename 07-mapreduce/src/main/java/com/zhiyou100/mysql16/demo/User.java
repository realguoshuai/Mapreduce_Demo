package com.zhiyou100.mysql16.demo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class User implements WritableComparable<User>,DBWritable{
	private int id;
	private String name;
	private int age;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		 out.writeUTF(name);
		 out.writeInt(age);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		name = in.readUTF();
		age=in.readInt();
	}
	
	@Override
	public int compareTo(User o) {
		//按照id排序
		return id-o.getId();
	}
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		// 向数据库中写数据,使用statement 填充 ?代表的字段
		statement.setInt(1, id);
		statement.setString(2, name);
		statement.setInt(3, age);
	}
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		// 从数据库读数据,使用结果集
		id=resultSet.getInt(1);
		name=resultSet.getString(2);
		age=resultSet.getInt(3);
	}

	
	@Override
	public String toString() {
		return "User [id=" + id + ", name=" + name + ", age=" + age + "]";
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

}
