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

public class GenderWiseIncome {

	/**
	 *3.... FIND THE TOTAL SUM OF INCOME BASED ON GENDER
	 */
	public static class MyMapper extends
	Mapper<LongWritable, Text, Text, FloatWritable> {
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String arr[] = value.toString().split(",");
	context.write(new Text(arr[3]),
			new FloatWritable(Float.parseFloat(arr[5])));
}
}

public static class MyReducer extends
	Reducer<Text, FloatWritable, Text, FloatWritable> {


public void reduce(Text key, Iterable<FloatWritable> value,
		Context context) throws IOException, InterruptedException {
	float sum = 0;

	for (FloatWritable a : value) {
		
sum=sum+a.get();
	}
	
	context.write(key, new FloatWritable(sum));
}



}

public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException {
Configuration c = new Configuration();
Job job = Job.getInstance(c, "gender wise income");
job.setJarByClass(GenderWiseIncome.class);
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
