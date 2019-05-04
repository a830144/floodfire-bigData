package home3;

import home2.CrimeFirstLastDateDriver;
import home2.CrimeFirstLastDateMapper;
import home2.CrimeFirstLastDateReducer;
import home2.FirstDateLastDateTuple;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Top5LongestTweetsDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		JobControl jobControl = new JobControl("jobChain");
		Configuration conf1 = getConf();

		Job job1 = Job.getInstance(conf1);
		job1.setJarByClass(Top5LongestTweetsDriver.class);
		job1.setJobName("top5");
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[0] + "/temp"));
		
		job1.setMapperClass(Top5Mapper.class);
		job1.setReducerClass(Top5Reducer.class);
		job1.setCombinerClass(Top5Reducer.class);

		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setNumReduceTasks(1);

		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);

		jobControl.addJob(controlledJob1);
		Configuration conf2 = getConf();

		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(Top5LongestTweetsDriver.class);
		job2.setJobName("join");
		
		MultipleInputs.addInputPath(job2, new Path(args[0] + "/temp"),
				TextInputFormat.class, TweetMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]),
				TextInputFormat.class, UserMapper.class);

		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.setReducerClass(UserJoinReducer.class);
		//job2.setCombinerClass(UserJoinReducer.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(job2);

		// make job2 dependent on job1
		controlledJob2.addDependingJob(controlledJob1);
		// add the job to the job control
		jobControl.addJob(controlledJob2);
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();

		while (!jobControl.allFinished()) {
			System.out.println("Jobs in waiting state: "
					+ jobControl.getWaitingJobList().size());
			System.out.println("Jobs in ready state: "
					+ jobControl.getReadyJobsList().size());
			System.out.println("Jobs in running state: "
					+ jobControl.getRunningJobList().size());
			System.out.println("Jobs in success state: "
					+ jobControl.getSuccessfulJobList().size());
			System.out.println("Jobs in failed state: "
					+ jobControl.getFailedJobList().size());
			try {
				Thread.sleep(5000);
			} catch (Exception e) {

			}

		}
		System.exit(0);
		return (job1.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Top5LongestTweetsDriver(), args);
		System.exit(exitCode);
		/*try {
			Job job = Job.getInstance(new Configuration(), "top5");
			job.setJarByClass(Top5LongestTweetsDriver.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setMapperClass(Top5Mapper.class);
			job.setCombinerClass(Top5Reducer.class);
			job.setReducerClass(Top5Reducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setNumReduceTasks(1);
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
		}*/
	}

}
