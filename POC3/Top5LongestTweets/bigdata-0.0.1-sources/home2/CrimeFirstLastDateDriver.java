package home2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeFirstLastDateDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Job job = Job.getInstance(new Configuration(), "crime first date and last date");
			job.setJarByClass(CrimeFirstLastDateDriver.class);
			job.setMapperClass(CrimeFirstLastDateMapper.class);
			job.setCombinerClass(CrimeFirstLastDateReducer.class);
			job.setReducerClass(CrimeFirstLastDateReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FirstDateLastDateTuple.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
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
