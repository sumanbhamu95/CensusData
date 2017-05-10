package Education;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Education.MaxSalaryEduac6.MyMapper;
import Education.MaxSalaryEduac6.MyReducer;

public class EducationTheirIncome7 {

	/**
	 * @param args
	 */
	
	public static class MyMapper extends
	Mapper<LongWritable, Text, Text, FloatWritable> {
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String arr[] = value.toString().split(",");
	context.write(new Text(arr[1]),
			new FloatWritable(Float.parseFloat(arr[5])));
}
}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration c = new Configuration();
			Job job = Job.getInstance(c, " Education and their income ");
			job.setJarByClass(EducationTheirIncome7.class);
			job.setMapperClass(MyMapper.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FloatWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileSystem.get(c).delete(new Path(args[1]), true);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
