package home3;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import class3.MRDPUtils;

public class Top5Reducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {
	private TreeMap<Integer, Text> lengthToRecordMap = new TreeMap<Integer, Text>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String[] parsed = value.toString().split("\t");
			lengthToRecordMap.put(parsed[4].length(), new Text(value));

			if (lengthToRecordMap.size() > 5) {
				lengthToRecordMap.remove(lengthToRecordMap.firstKey());
			}
			//System.out.println(lengthToRecordMap.size());
		}
		
		for (Text t : lengthToRecordMap.descendingMap().values()) {
			context.write(NullWritable.get(), t);
			System.out.println(t);
		}
	}
}
