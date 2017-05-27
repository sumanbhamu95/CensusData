package FinanCensus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SumOfTax1 {

	/**
	 * @param args
	 */
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			float inc = Float.parseFloat(arr[5]);
			context.write(new Text("Total tax .."), new FloatWritable(inc));
		}
	}

	public static class MyReducer extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> value,
				Context context) throws IOException, InterruptedException {
			float c = 0,sum=0;

			for (FloatWritable a : value) {
				c = a.get() / 10;
				sum=sum+c;
			}

			context.write(key, new FloatWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();
		Job job = Job.getInstance(c, "count sum of paying tax");
		job.setJarByClass(SumOfTax1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		FileSystem.get(c).delete(new Path(args[1]), true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
