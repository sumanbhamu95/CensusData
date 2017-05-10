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

public class EducatedFromNativeForeign5 {

	/**
	 * educated people from native and foreign
	 */
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			String assu = arr[1] + "," + arr[8];
			
			context.write(new Text("total"), new Text(assu));
		}
	}

	public static class MyPartitioner extends Partitioner<Text, Text> {

		public int getPartition(Text key, Text value, int repo) {
			if (value.toString().contains("Native")) {
				
				return 0;
			}
			if (value.toString().contains("Foreignborn")) {
				return 1;
			}

			else {
				return 2;
			}

		}

	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {

			int x = 0;
			for (Text s : value) {
				x++;
			}
			context.write(key, new IntWritable(x));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();

		Job job = Job.getInstance(c, "educated from native and foreign");
		job.setJarByClass(EducatedFromNativeForeign5.class);
		job.setMapperClass(MyMapper.class);

		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(3);
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
