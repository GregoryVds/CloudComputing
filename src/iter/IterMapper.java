package iter;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException 
	{
		String line = value.toString();
		String[] splited = line.split("\\s+");
			
		String nodeId 	= splited[0];
		int iterNbr 	= Integer.parseInt(splited[1]);
		int distance  	= Integer.parseInt(splited[2]);
		String adjList 	= splited[3];
	
		// Propagate distances.
		Integer newDist;
		if (distance!=Integer.MAX_VALUE) {
			// Substring(1) trims the leading ":"
			for (String ajdVertex : adjList.substring(1).split(":")) {
				newDist = distance+1;
				context.write(new Text(ajdVertex), new Text(newDist.toString()));
			}
		}
		
		// Pass along graph structure.
		String nodeRep = (iterNbr+1) + " " + distance + " " + adjList;
		context.write(new Text(nodeId), new Text(nodeRep));
	}	
}