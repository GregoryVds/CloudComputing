package iter;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	 @Override
	    protected void reduce(Text key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException
	    {
		 	String val;
		 	String[] splitted;
		 	int minNewDistance = Integer.MAX_VALUE;
		 	String adjList = "";
		 	int iterNbr = 0;
		 	
	        for (Text value : values) {
	        	val = value.toString();
	        	splitted = val.split("\\s+");
	        	
	        	// If length > 1, then this is the node structure 
	        	// that was passed along with the following format:
	        	// nodeNumber => iterNbr oldDistance adjList
	        	if (splitted.length > 1) {
	        		minNewDistance = Math.min(Integer.parseInt(splitted[1]), minNewDistance);
	        		iterNbr = Integer.parseInt(splitted[0]);
	        		adjList = splitted[2];
	        	} 
	        	// Else, it is an updated distance to reach this node.
	        	else
	        		minNewDistance = Math.min(Integer.parseInt(val), minNewDistance);
	        }
	        
	        // The intermediate format contains one line per node with
	        // the following information:
	        // "nodeNumber iterationNumber distanceFromSource ajdList"
			String nodeRep = iterNbr + " " + minNewDistance + " " + adjList;
	        context.write(key, new Text(nodeRep));
	    }	
}