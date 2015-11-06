// Author: Gregory Vander Schueren
// Date: November 3, 2015

package evaluate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class EvaluateMapper extends Mapper<LongWritable, Text, Text, Text> {
	String COMMON_KEY = "x";
	
	public Text evaluateFormat(int iterNbr, String nodeId, int distance, String pathToNode) {
		return new Text(iterNbr+" "+nodeId+" "+Integer.toString(distance)+" "+pathToNode);
	}
	
	/*
	Each line from the input data represents a node of a the graph
	and contains the following information:
	"nodeId iterationNumber distance pathToNode adjList".
	
	For each node N reachable in distance D (D != Integer.MAX_VALUE), if the destination
	node is in the adjacency list, then we output that it is reachable in distance D 
	via the path to reach N after X iterations.
	 
	We use a common dummy reduce key to ensure that only 1 reduce is used in order 
	to find the minimum value among all paths.
	*/
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException 
	{
		String[] splitted = value.toString().split("\\s+");
		
		// Get destination id
    	Configuration conf 	 = context.getConfiguration();
        String destinationId = conf.get("dstId");
        
        String nodeId 		= splitted[0];
        int iterNbr 		= Integer.parseInt(splitted[1]);
		int distance  		= Integer.parseInt(splitted[2]);
		String pathToNode 	= splitted[3];
		String adjList 		= splitted[4];
		
		// We output to node only if it is reachable.
		if (distance!=Integer.MAX_VALUE) {
			// Substring(1) trims the leading ":"
			for (String ajdVertex : adjList.substring(1).split(":")) {
				// Destination is reachable via nodeId.
				if (ajdVertex.equals(destinationId))
					context.write(new Text(COMMON_KEY), evaluateFormat(iterNbr, nodeId, distance, pathToNode));
			}
		}
		
		// Dummy neighbor. We want the reduce to be invoked even if we don't have any result.
		// since we need to output an empty array. We ensure it is sent only once by checking that
		// the current node is the destination.
		if (nodeId.equals(destinationId))
			context.write(new Text(COMMON_KEY), evaluateFormat(iterNbr, nodeId, Integer.MAX_VALUE, pathToNode)); 
	}	
}