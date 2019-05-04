package home1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NeighborhoodCrimeCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text neighborhood = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] result = value.toString().split(",");
		neighborhood.set(result[7]);
		context.write(neighborhood, one);
	}
}
