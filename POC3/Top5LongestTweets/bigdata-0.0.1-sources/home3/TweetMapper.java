package home3;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<Object, Text, Text, Text> {
	private Text outkey = new Text();
	private Text outvalue = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] parsed = value.toString().split("\t");
		outkey.set(parsed[1]);
		outvalue.set("A" + parsed[0]+"\t"+parsed[4]+"\t"+parsed[1]);
		context.write(outkey, outvalue);
	}

	
}
