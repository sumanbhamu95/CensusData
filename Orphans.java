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



public class Orphans {
	public static class ma extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			
			
			String b=arr[3];
			if(arr[6].toString().contains("Notinuniverse"))
			context.write(new Text(b), new IntWritable(Integer.parseInt(arr[9])));
			
		}
	}
	public static class re extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			int count=0;
			int sum=0;
			for(IntWritable a:value){
				if(a.get()==0){
					count++;
				}
				
			}
			
			context.write(key, new IntWritable(count));
			
		}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"country orphans");
		job.setJarByClass(Orphans.class);
		job.setMapperClass(ma.class);
	job.setReducerClass(re.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	//	job.setNumReduceTasks(1);
		 job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(c).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	     		
		
	}

}


   

