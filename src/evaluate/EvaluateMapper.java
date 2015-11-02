package evaluate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class EvaluateMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException 
	{
		String line = value.toString();
		String[] splited = line.split("\\s+");
		
		// Get destination id
    	Configuration conf 	 = context.getConfiguration();
        String destinationId = conf.get("dstId");
        
		String nodeId 	= splited[0];
		int distance  	= Integer.parseInt(splited[1]);
		String adjList 	= splited[2];
		
		if (distance!=Integer.MAX_VALUE) {
			// Substring(1) trims the leading ":"
			for (String ajdVertex : adjList.substring(1).split(":")) {
				// Destination is reachable via nodeId.
				if (ajdVertex.equals(destinationId)) 
					context.write(new Text("x"), new Text(nodeId+" "+Integer.toString(distance)));
			}
		}
	}	
}