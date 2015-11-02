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
    	String neighbours = "";
    	
    	String[] splitted;
    	String val, node;
    	int distance;
    	    	
        for (Text value : values) {
        	val 		= value.toString();
        	splitted 	= val.split("\\s+");
        	node 		= splitted[0];
        	distance 	= Integer.parseInt(splitted[1]);
        	System.out.println("Distance:"+distance);
        	
        	if (distance < minDistance) {
        		neighbours = "[" + node;
        		minDistance = distance;
        	}
        	else if (distance == minDistance)
        		neighbours += ("," + node);
        }

        neighbours+="]";
        context.write(new Text(Integer.toString(minDistance)), new Text(neighbours));
        
    }
}