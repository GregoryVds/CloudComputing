// Author: Gregory Vander Schueren
// Date: November 3, 2015

package iter;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import be_uclouvain_ingi2145_p1.Utils;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	/*
	Each line from the input data represents a node of a the graph
	and contains the following information:
	"nodeId iterationNumber distance pathToNode adjList".
	
	For each node N, we loop over all its adjacent nodes A. If the current node is
	reachable at distance D, then we output that node A is reachable at distance D+1
	with path being PathToN + N.
	
	Then, we also output once the graph structure to pass it along for the next
	iteration. 
	*/
	
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException 
	{
		// Simply split the input line to extract the information.
		String[] splited = value.toString().split("\\s+");
			
		String nodeId 		= splited[0];
		int iterNbr 		= Integer.parseInt(splited[1]);
		int distance  		= Integer.parseInt(splited[2]);
		String pathToNode 	= splited[3];
		String adjList 		= splited[4];
			
		// Expand discovery front from the current node.
		Integer newDist;
		String newPath;
		if (distance!=Integer.MAX_VALUE) { // Only if current node is reachable.
			// Substring(1) trims the leading ":"
			for (String ajdVertex : adjList.substring(1).split(":")) {
				newDist = distance+1;
				newPath = pathToNode + nodeId + ":";
				context.write(new Text(ajdVertex), new Text(newDist.toString() + " " + newPath));
			}
		}
		
		// Pass along the graph structure while incrementing the iteration number.
		context.write(new Text(nodeId), Utils.buildIntermediateRep(iterNbr+1, distance, pathToNode, adjList));
	}	
}