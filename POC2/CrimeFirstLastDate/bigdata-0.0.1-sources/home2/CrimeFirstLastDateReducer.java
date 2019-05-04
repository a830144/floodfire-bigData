package home2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimeFirstLastDateReducer extends
Reducer<Text, FirstDateLastDateTuple, Text, FirstDateLastDateTuple>{
	private FirstDateLastDateTuple result = new FirstDateLastDateTuple();
	public void reduce(Text key, Iterable<FirstDateLastDateTuple> values,
			Context context) {
		result.setMin(null);
		result.setMax(null);
		for (FirstDateLastDateTuple val : values) {
			if (result.getMin() == null
					|| val.getMin().compareTo(result.getMin()) < 0) {
				result.setMin(val.getMin());
			}
			if (result.getMax() == null
					|| val.getMax().compareTo(result.getMax()) > 0) {
				result.setMax(val.getMax());
			}
		}
		try {
			context.write(key, result);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
