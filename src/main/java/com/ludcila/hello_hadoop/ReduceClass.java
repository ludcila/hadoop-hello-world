package com.ludcila.hello_hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		ArrayList<String> words = new ArrayList<String>();
		
		Iterator<Text> valuesIt = values.iterator();
		while(valuesIt.hasNext()) {
			words.add(valuesIt.next().toString());
		}
		context.write(key, new Text(String.join(", ", words)));
		
	}

}
