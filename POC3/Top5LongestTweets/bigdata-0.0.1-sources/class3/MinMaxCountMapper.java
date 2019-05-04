package class3;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends
		Mapper<Object, Text, Text, MinMaxCountTuple> {
	private Text outUserId = new Text();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS");

	public void map(Object key, Text value, Context context) {
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
				.toString());
		String strDate = parsed.get("CreationDate");
		String userId = parsed.get("OwnerUserId");
		System.out.println("createDate"+parsed.get("CreationDate"));
		System.out.println("ownerUserId"+parsed.get("OwnerUserId"));
		Iterator iter = parsed.keySet().iterator();
		while(iter.hasNext()){
			System.out.println(iter.next());
		}
		try {
			Date creationDate = frmt.parse(strDate);
			outTuple.setMin(creationDate);
			outTuple.setMax(creationDate);
			outTuple.setCount(1);
			if(userId==null || userId.equals("")){
				outUserId.set("0000");
			}else{
				outUserId.set(userId);
			}
			context.write(outUserId, outTuple);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}