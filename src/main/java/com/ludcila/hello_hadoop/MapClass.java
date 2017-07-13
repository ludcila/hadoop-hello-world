package com.ludcila.hello_hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	private Text word = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().replace('.', ' ').replace(',', ' ');
		StringTokenizer st = new StringTokenizer(line, " ");
		while(st.hasMoreTokens()) {
			word.set(st.nextToken());
			context.write(new IntWritable(word.getLength()), word);
		}
	}
	
}
