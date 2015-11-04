package evaluate;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class EvaluateReducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {       	
    	int minDistance = Integer.MAX_VALUE;
    	String neighbors = "";
    	String[] splitted;
    	String val, node;
    	int iterNbr = 0;
    	int distance;
    	 	
        for (Text value : values) {
        	val 		= value.toString();
        	splitted 	= val.split("\\s+");
        	node 		= splitted[0];
        	iterNbr		= Integer.parseInt(splitted[1]);
        	distance 	= Integer.parseInt(splitted[2]);
        	
        	if (distance == Integer.MAX_VALUE) {} // Don't do anything. This is the dummy neighbor.
        	else if (distance < minDistance) {
        		minDistance = distance;
        		neighbors = "[" + node;
        	}
        	else if (distance == minDistance)
        		neighbors += (", " + node);
        }
        
        // If neighbors exist, just closed list. Otherwise output empty list.
        if (neighbors.length() != 0) 
        	neighbors+="]";
        else
        	neighbors="[]";
        
        // Number of iterations = distance-1 since we do N-2 iterations.
        
        context.write(new Text(Integer.toString(iterNbr)), new Text(neighbors));
    }
}