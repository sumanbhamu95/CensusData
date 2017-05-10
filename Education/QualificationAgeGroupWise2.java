package Education;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QualificationAgeGroupWise2 {

	/**
	 * highest qualification in each age grp
	 */
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			context.write(new Text(arr[1]),
					new IntWritable(Integer.parseInt(arr[0])));
		}
	}

	public static class MyPartitioner extends Partitioner<Text, IntWritable> {

		public int getPartition(Text key, IntWritable value, int repo) {
			if (value.get() <= 10) {
				return 0;
			}
			if (value.get() > 10 && value.get() <= 20) {
				return 1;
			}
			if (value.get() > 20 && value.get() <= 30) {
				return 2;
			}
			if (value.get() > 30 && value.get() <= 40) {
				return 3;
			}
			if (value.get() > 40 && value.get() <= 50) {
				return 4;
			}
			if (value.get() > 50 && value.get() <= 60) {
				return 5;
			}
			if (value.get() > 60 && value.get() <= 70) {
				return 6;
			}
			if (value.get() > 70 && value.get() <= 80) {
				return 7;
			}
			if (value.get() > 80 && value.get() <= 90) {
				return 8;
			} else {
				return 9;
			}

		}

	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		

		int max = 0;
		Text maxWord = new Text();

		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {
			int count = 0;

			for (IntWritable a : value) {
				count++;

			}
			if (count > max) {
				max = count;
				maxWord.set(key);
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			context.write(maxWord, new IntWritable(max));
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();
		Job job = Job.getInstance(c, "Qualification in each age grp");
		job.setJarByClass(QualificationAgeGroupWise2.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileSystem.get(c).delete(new Path(args[1]), true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
