package init;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        // Get origin id
    	Configuration conf = context.getConfiguration();
        String sourceId = conf.get("srcId");
              
        // Define helper variables
        boolean isAjdToOrigin = false;
        boolean isOrigin = key.toString().equals(sourceId);
        
    	// We build an adjacency list for each node.
    	// The adjacency list has the form ":2:3:4"
        // where 2,3,4 are indices of adjacent nodes.
    	String ajdList = "";
        for (Text value : values) {
        	if (sourceId.equals(value.toString())) isAjdToOrigin = true;
        	
        	// Knowing this will allow us to initiate the distance at 1
        	// for nodes adjacent to source and skip one MapReduce iteration.
        	ajdList+=(":"+value.toString());
        }
        
        String distance;
        if (isOrigin)
        	distance = "0";
        else if (isAjdToOrigin)
        	distance = "1";
        else
        	distance = "-1";
              	 
        // We output "NodeNumber" => "DistanceFromSource AdjList"
        // Example: "12" => "14 3:14:53"
        String nodeRep = distance + " " + ajdList;         
        context.write(key, new Text(nodeRep));
    }
}