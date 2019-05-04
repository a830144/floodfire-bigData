package home2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeFirstLastDateMapper extends
		Mapper<LongWritable, Text, Text, FirstDateLastDateTuple> {
	private Text crime_type = new Text();
	private FirstDateLastDateTuple outTuple = new FirstDateLastDateTuple();
	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"YYYY-mm-dd");

	public void map(LongWritable key, Text value, Context context) {
		String[] result = value.toString().split(",");
		crime_type.set(result[0]);

		try {
			String strDate = result[1] +"-"+ result[2] + "-"+ result[3];
			Date creationDate = frmt.parse(strDate);
			outTuple.setMin(creationDate);
			outTuple.setMax(creationDate);
			context.write(crime_type, outTuple);
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
