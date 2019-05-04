package home1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeTypeCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text crime_type = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] result = value.toString().split(",");
		crime_type.set(result[0]);
		context.write(crime_type, one);
	}
}
