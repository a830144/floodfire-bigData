package home3;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UserMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] parsed = value.toString().split("\t");
		outkey.set(parsed[0]);
		outvalue.set("B" +parsed[3]);
		context.write(outkey, outvalue);
	}
}
