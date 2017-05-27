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

public class P3 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			int age=Integer.parseInt(arr[0]);
			int work=Integer.parseInt(arr[9]);
			
			if(age>23 && work==0)
			context.write(new Text("total no of unemployed"), new Text(arr[2]));

		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (Text a : value) {

				
					count++;

			
			}
			context.write(key, new IntWritable(count));
		}
	}

	public static void main(String args[]) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration obj = new Configuration();
		Job job = Job.getInstance(obj, "citizen age >23 educated and unemploy..");
		job.setJarByClass(P3.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(obj).delete(new Path(args[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
