package init;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException 
	{
		// Each value represents an edge and has the form "1 2" 
		// where 1 and 2 are the two nodes connected by the edge.
		String line = value.toString();
		String[] splited = line.split(" ");

		context.write(new Text(splited[0]), new Text(splited[1]));
		context.write(new Text(splited[1]), new Text(splited[0]));
	}	
}