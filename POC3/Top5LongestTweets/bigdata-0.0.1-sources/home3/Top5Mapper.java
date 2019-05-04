package home3;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * input : UID/TID/length/followers
 * @author cloudera
 *
 */

public class Top5Mapper extends Mapper<Object, Text, NullWritable, Text> {
	private TreeMap<Integer, Text> lengthToRecordMap = new TreeMap<Integer, Text>();

	public void map(Object key, Text value, Context context) {
		String[] parsed = value.toString().split("\t");
		lengthToRecordMap.put(parsed[4].length(), new Text(value));
		//System.out.println(Integer.parseInt(parsed[2]));
		if (lengthToRecordMap.size() > 5) {
			lengthToRecordMap.remove(lengthToRecordMap.firstKey());
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Text t : lengthToRecordMap.descendingMap().values()) {
			//System.out.println(t);
			 context.write(NullWritable.get(), t);
			 
		}
	}
}
