package evaluate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class EvaluateMapper extends Mapper<LongWritable, Text, Text, Text> {
	String COMMON_KEY = "x";
	
	public Text evaluateFormat(int iterNbr, String nodeId, int distance) {
		return new Text(iterNbr+" "+nodeId+" "+Integer.toString(distance));
	}
	
			
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException 
	{
		String line = value.toString();
		String[] splited = line.split("\\s+");
		
		// Get destination id
    	Configuration conf 	 = context.getConfiguration();
        String destinationId = conf.get("dstId");
        
        int iterNbr 	= Integer.parseInt(splited[0]);
        String nodeId 	= splited[1];
		int distance  	= Integer.parseInt(splited[2]);
		String adjList 	= splited[3];
		
		if (distance!=Integer.MAX_VALUE) {
			// Substring(1) trims the leading ":"
			for (String ajdVertex : adjList.substring(1).split(":")) {
				// Destination is reachable via nodeId.
				if (ajdVertex.equals(destinationId))
					context.write(new Text(COMMON_KEY), evaluateFormat(iterNbr, nodeId, distance));
			}
		}
		
		// Dummy neighbor. We want the reduce to be invoked even if we don't have any result.
		// Send only once.
		if (nodeId==destinationId)
			context.write(new Text(COMMON_KEY), evaluateFormat(iterNbr, nodeId, Integer.MAX_VALUE)); 
	}	
}