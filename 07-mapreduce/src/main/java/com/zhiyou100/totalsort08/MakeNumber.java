package com.zhiyou100.totalsort08;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class MakeNumber {

	public static void main(String[] args) {

		try (FileWriter fileWriter = new FileWriter("E:/number.log");
				BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)){
			
			int max = 10000;
			
			for (int i = 0; i < 1000000; i++) {
				
				int number = (int)(Math.random() * max);
				
				bufferedWriter.write(String.valueOf(number));
				bufferedWriter.newLine();
			}
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
}
