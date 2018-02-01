package com.zhiyou100.mysql16.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class Employee implements WritableComparable<Employee>,DBWritable{
	private int id;
	private String name;
	private int age;
	private int department;
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(name);
		out.writeInt(age);
		out.writeInt(department);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		id=in.readInt();
		name=in.readUTF();
		age=in.readInt();
		department=in.readInt();
		
	}
	@Override
	public int compareTo(Employee o) {
		//按照id排序
		return id-o.getId();
	}
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setInt(1, id);
		statement.setString(2, name);
		statement.setInt(3, age);
		statement.setInt(4, department);
		
	}
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		id=resultSet.getInt(1);
		name=resultSet.getString(2);
		age=resultSet.getInt(3);
		department=resultSet.getInt(4);
		
	}
	
	@Override
	public String toString() {
		return "Employee [id=" + id + ", name=" + name + ", age=" + age + ", department=" + department + "]";
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
	public int getDepartment() {
		return department;
	}
	public void setDepartment(int department) {
		this.department = department;
	}
	
	
}
