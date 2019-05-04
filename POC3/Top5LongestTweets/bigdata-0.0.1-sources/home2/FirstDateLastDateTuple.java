package home2;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;


public class FirstDateLastDateTuple implements Writable {
	private Date min = new Date();
	private Date max = new Date();

	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"YYYY-mm-dd");

	public Date getMin() {
		return min;
	}

	public void setMin(Date min) {
		this.min = min;
	}

	public Date getMax() {
		return max;
	}

	public void setMax(Date max) {
		this.max = max;
	}

	

	public void readFields(DataInput in) {
		try {
			min = new Date(in.readLong());
			max = new Date(in.readLong());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void write(DataOutput out) {
		try {
			out.writeLong(min.getTime());
			out.writeLong(max.getTime());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String toString() {
		return frmt.format(min) + "\t" + frmt.format(max);
	}
}
