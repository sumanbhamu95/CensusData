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

public class CategoryWiseEmployedOrNot3 {

	/**
	 * no. of candidates educated category wise employed or not
	 */
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			context.write(new Text(arr[1]),
					new IntWritable(Integer.parseInt(arr[9])));
		}
	}

	
	  public static class MyPartitioner extends Partitioner<Text, IntWritable>
	  {
	 
	  public int getPartition(Text key, IntWritable value, int repo) 
	  { if(value.get() != 0) {// employed 
		  return 0;
	  }
		  
	 if (value.get() == 0) //unemployed
	  { 
		  return 1;
		  } else
		  { return 2; 
		  }
	  
	  }
	  
	  }
	 
	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {

			int c = 0;
			for (IntWritable s : value) {

				/*if (!((key.toString().equals("Children")) || (s.get() == 0))) {
					c++;
				}*/

				 c++;
			
			}

			context.write(key, new IntWritable(c));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();

		Job job = Job.getInstance(c, "Category wise employed or not");
		job.setJarByClass(CategoryWiseEmployedOrNot3.class);
		job.setMapperClass(MyMapper.class);

		job.setReducerClass(MyReducer.class);
		
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(3);
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
