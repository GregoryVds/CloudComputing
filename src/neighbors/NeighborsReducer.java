// Author: Gregory Vander Schueren
// Date: November 3, 2015

package neighbors;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class NeighborsReducer extends Reducer<Text, Text, Text, Text> 
{	
	/*
    This reduce phase outputs the computation current result neighbors in a file.
    The reduce key is a dummy key.
    The values lists all the paths to PoI adjacent to the destination node.
    plus an additional dummy path with infinite distance to ensure this reducer is called.
    
    Each value has the following form:
    "dummyKey => iterNbr nodeId distance pathToNode"
    */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {    
    	// We don't want to allocate variables in the loop for speedup, so we do it here.
    	int minDistance 	= Integer.MAX_VALUE;
    	String neighbors 	= "";
    	String[] splitted;
    	String[] splittedPath;
    	int distance;
    	 	
        for (Text value : values) {
        	splitted 	 = value.toString().split("\\s+");
        	distance 	 = Integer.parseInt(splitted[2]);
        	splittedPath = splitted[3].split(":");
        	
        	if (distance == Integer.MAX_VALUE) {} // Don't do anything. This is the dummy neighbor.
        	else if (distance < minDistance) {
        		minDistance = distance;
        		neighbors = "[" + splittedPath[2];
        	}
        	else if (distance == minDistance)
        		neighbors += ("," + splittedPath[2]);
        }
        
        // If neighbors exist, just close the list. Otherwise output empty list.
        if (neighbors.length() != 0) 
        	neighbors+="]";
        else
        	neighbors="[]";
        
        context.write(new Text(neighbors), new Text(""));
    }
}