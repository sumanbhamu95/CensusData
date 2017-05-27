package FinanCensus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NonCitiWorkingPercen6 {

	/**
	 * NUMBER OF NON CITIZEN WORKING PERCENTAGE
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			String nonCiti = arr[8];
			int work = Integer.parseInt(arr[9]);

			if ((!nonCiti.equals("Native-BornintheUnitedStates")))
				context.write(new Text("non citizen working percen.."),
						new Text(arr[8] + "," + arr[9]));
		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			int c = 0, p = 0,pr=0;

			for (Text a : value) {
				c++;
				String arr[] = a.toString().split(",");
				
				int work = Integer.parseInt(arr[1]);
				if(work>0)
				p++;

			}

			pr=(p*100)/c;
			context.write(key, new IntWritable(pr));
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();
		Job job = Job.getInstance(c, "count no. of non working percent");
		job.setJarByClass(NonCitiWorkingPercen6.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileSystem.get(c).delete(new Path(args[1]), true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
