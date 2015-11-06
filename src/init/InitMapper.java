// Author: Gregory Vander Schueren
// Date: November 3, 2015

package init;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	/*
	Each line from the input data represents an edge of the graph.
	A line has the form "1 2" where 1 and 2 are the two nodes connected by the edge.

	For each node, we want to collect all the adjacent nodes to build
	its adjacency list. We will use the node number as the reduce key
	and the adjacent node as the value.
	*/
	
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException 
	{
		// Simply split the line to get Node1 and Node2.
		String[] splitted = value.toString().split("\\s+");

		// Since the since the graph is undirected, we
		// output twice this edge, in both direction.
		context.write(new Text(splitted[0]), new Text(splitted[1]));
		context.write(new Text(splitted[1]), new Text(splitted[0]));
	}	
}