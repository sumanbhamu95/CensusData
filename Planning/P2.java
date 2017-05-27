package FinanCensus;

import java.io.IOException;
import java.util.Scanner;

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

public class P2 {

	/**
	 * @param args
	 */
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			String year = context.getConfiguration().get("yy");
			int a = Integer.parseInt(arr[0]) + Integer.parseInt(year);

			context.write(new Text(arr[3]), new IntWritable(a));
		}
	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {

			int x = 0;
			for (IntWritable s : value) {

				if (s.get() > 65) {
					x++;
				}

			}

			String key1 = key + " no. of senior citizen..";
			context.write(new Text(key1), new IntWritable(x));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();

		String year;
		System.out.println("enter  an year");
		Scanner sc = new Scanner(System.in);
		year = sc.next();
		c.set("yy", year);

		Job job = Job.getInstance(c, "Count senior citezen");
		job.setJarByClass(P2.class);
		job.setMapperClass(MyMapper.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileSystem.get(c).delete(new Path(args[1]), true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
