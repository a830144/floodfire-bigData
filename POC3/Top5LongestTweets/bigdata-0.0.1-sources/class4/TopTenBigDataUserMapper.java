package class4;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import class3.MRDPUtils;

public class TopTenBigDataUserMapper extends
		Mapper<Object, Text, NullWritable, Text> {
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
				.toString());
		String reputation = parsed.get("Reputation");
		String aboutme = parsed.get("AboutMe");
		if (reputation == null || aboutme == null) {
			return;
		}
		String term = "big data";
		if (aboutme.toLowerCase().contains(term)) {
			repToRecordMap.put(Integer.parseInt(reputation), new Text(value));
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Text t : repToRecordMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}
}
