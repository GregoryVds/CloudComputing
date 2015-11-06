// Author: Gregory Vander Schueren
// Date: November 3, 2015

package iter;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import be_uclouvain_ingi2145_p1.Utils;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	/*
	This reduce phase will save the graph data into our intermediate format.
	Before saving, we update the distance and pathToNode if necessary.
	
	The input format may be in 2 forms.
	Either it is a potential new distance+path for a node:
		"nodeId => distance pathToNode"
	Else it is simply the graph structure that is passed along:
	 	 "nodeId => iterationNumber distance pathToNode adjList"
	*/
	
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
		// We don't want to allocate variables in the loop for speedup, so we do it here.
		int newDistance 	 = Integer.MAX_VALUE;
		int iterNbr 		 = 0;
		int distance 		 = 0;
		String adjList 		 = "";
		String newPathToNode = "";
	 	String[] splitted;
	 	
        for (Text value : values) {
        	splitted = value.toString().split("\\s+");
        	
        	// If length > 2, then this is the node structure that was passed along.
        	if (splitted.length > 2) {
        		iterNbr		= Integer.parseInt(splitted[0]);
        		adjList		= splitted[3];
        		distance	= Integer.parseInt(splitted[1]);
        		// If the distance if better that what was found so far, record it and the path.
        		if (distance <= newDistance) {
        			newDistance    = distance;
        			newPathToNode  = splitted[2];
        		}
        	} 
        	// Else, it is another potential distance to reach this node.
        	else {
        		distance = Integer.parseInt(splitted[0]); 
        		// If the distance if better that what was found so far, record it and the path.
        		if (distance < newDistance) {
        			newDistance   = distance;
        			newPathToNode = splitted[1];
        		}
        	}
        }
                
        // Reduce in our intermediate representation.
        context.write(key, Utils.buildIntermediateRep(iterNbr, newDistance, newPathToNode, adjList));
    }	
}