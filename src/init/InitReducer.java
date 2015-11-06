// Author: Gregory Vander Schueren
// Date: November 3, 2015

package init;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

import be_uclouvain_ingi2145_p1.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    /*
    This reduce phase will save the graph data into our intermediate format.
    The reduce key is the node id, and values is a list of adjacent nodes.
    The intermediate format contains 1 line per node. For each node, we have the following information:
    "nodeId iterationNumber distance pathToNode adjList".
    
     - "nodeId" is simply an integer representing the node id.
     - "iterationNumber" is an integer giving the number of iterations performed so far. We start at 0.
        This information is actually not needed for the main algorithm but only needed to produce the 
        output asked for the evaluate() function. In my opinion this information doesn't belong to the 
        files written to disk but I we have not other option if we want to test evaluate() in isolation.
        It would have been cleaner to pass this as an argument to iterate()} but we were not allowed to
        modify the method signature. Note that we could also have added an additional key in the 
        intermediate data format containing only meta-information such as the iteration number instead of
        adding it to every line.
     - "distance" is an integer giving the node distance from the origin. If the distance is not known
        yet, this field has a value of Integer.MAX\_VALUE. 
     - "pathToNode" is the shortest path to reach the node from the origin if the node is reachable. It
     	has the following form if we can reach node 3 from 1 via 2: ":1:2:".
     - "adjList" is the list of adjacent nodes. This represents the graph structure and has to be
        passed along at each iteration of the algorithm. It has the following form if 3,4 and 5 are 
        adjacent nodes ":3:4:5".
    */
    
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        // Get origin id
    	Configuration conf = context.getConfiguration();
        String sourceId    = conf.get("srcId");
              
        // Define helper variables
        boolean isAjdToOrigin = false;
        boolean isOrigin = key.toString().equals(sourceId);
        
    	// We don't want to allocate variables in the loop for speedup, so we do it here.
        String adjList = "";
    	String val;
    	// We build an adjacency list for each node.
    	// The adjacency list has the form ":2:3:4" if 2,3,4 are indices of adjacent nodes.
    	for (Text value : values) {
        	val = value.toString();
        	adjList+=(":"+val);
        	
        	// If the node is AdjToOrigin we can initiate its distance at 1.
        	if (sourceId.equals(val)) isAjdToOrigin = true;
        }
        
        int distance;
        if (isOrigin) // Origin is at distance 0 of itself.
        	distance = 0; 
        else if (isAjdToOrigin) // An adjacent node is at distance 1 of origin.
        	distance = 1; 
        else // Any other node is at infinite distance from origin.
        	distance = Integer.MAX_VALUE; 
        
        String pathToNode;
        if (isAjdToOrigin) // A node adjacent to origin is reachable in 1 step from origin.
        	pathToNode = ":"+sourceId+":";
        else // Otherwise there is no path yet to reach this node.
        	pathToNode = ":";
        
        // Reduce in our intermediate representation.   
        context.write(key, Utils.buildIntermediateRep(0, distance, pathToNode, adjList));
    }
}