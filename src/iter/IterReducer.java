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
		 	
	        for (Text value : values) {
	        	val = value.toString();
	        	splitted = val.split("\\s+");
	        	
	        	// If length > 1, then this is the node structure 
	        	// that was passed along.
	        	if (splitted.length > 1) {
	        		minNewDistance = Math.min(Integer.parseInt(splitted[0]), minNewDistance);
	        		adjList = splitted[1];
	        	} 
	        	// Else, it is an updated distance to reach this node.
	        	else {
	        		minNewDistance = Math.min(Integer.parseInt(val), minNewDistance);
	        	}
	        }
	        
			String nodeRep = minNewDistance + " " + adjList;
			System.out.println(nodeRep);
	        context.write(key, new Text(nodeRep));
	    }	
}