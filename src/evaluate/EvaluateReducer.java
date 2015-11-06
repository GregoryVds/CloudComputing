// Author: Gregory Vander Schueren
// Date: November 3, 2015

package evaluate;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class EvaluateReducer extends Reducer<Text, Text, Text, Text>
{
	/*
    This reduce phase outputs the computation current result in a file.
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
    	System.out.println("called");
    	// We don't want to allocate variables in the loop for speedup, so we do it here.
    	int minDistance 	= Integer.MAX_VALUE;
    	String neighbors 	= "";
    	int iterNbr 		= 0;
    	String[] splitted;
    	String node;
    	int distance;
    	 	
        for (Text value : values) {
        	splitted 	= value.toString().split("\\s+");
        	iterNbr		= Integer.parseInt(splitted[0]);
        	node 		= splitted[1];
        	distance 	= Integer.parseInt(splitted[2]);
           	
        	if (distance == Integer.MAX_VALUE) {} // Don't do anything. This is the dummy neighbor.
        	else if (distance < minDistance) {
        		minDistance = distance;
        		neighbors = "[" + node;
        	}
        	else if (distance == minDistance)
        		neighbors += ("," + node);
        }
        
        // If neighbors exist, just close the list. Otherwise output empty list.
        if (neighbors.length() != 0) 
        	neighbors+="]";
        else
        	neighbors="[]";
        
        context.write(new Text(Integer.toString(iterNbr)), new Text(neighbors));
    }
}