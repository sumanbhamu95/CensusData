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

public class CountNonTaxPayer4 {

	/**
	 * FIND THE TOTAL NUMBER WHO ARE NOT PAYING Tax
	 */
	public static class MyMapper extends
	Mapper<LongWritable, Text, Text, Text> {
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String arr[] = value.toString().split(",");
	context.write(new Text("non tax payers.."),
			new Text(arr[4]));
}
}

public static class MyReducer extends
	Reducer<Text, Text, Text, IntWritable> {


public void reduce(Text key, Iterable<Text> value,
		Context context) throws IOException, InterruptedException {
	int c=0;

	for (Text a : value) {
		if(a.toString().contains("Nonfiler")){
			c++;
		}
	}
	
	context.write(key, new IntWritable(c));
}



}

public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException {
Configuration c = new Configuration();
Job job = Job.getInstance(c, "count no. of people who is  not paying tax");
job.setJarByClass(CountNonTaxPayer4.class);
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
