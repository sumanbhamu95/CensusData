package FinanCensus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SULITERACYRATE {

	/**
	 * @param args
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			String chil=arr[1];
			
			if(!chil.equals("Children"))
			context.write(new Text("literacy rate"), new Text(arr[3]+","+arr[1]));
		}
	}
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			
			int c=0,x=0,m=0;
			String p="";
			for(Text s:value){
				String a[]=s.toString().split(",");
				
				if(  (a[0].toString().equals("Female"))){
					x++;
				}
				else if((a[0].toString().equals("Male")))
						{
					m++;
						}
				c++;
			
			}
			p=(x*100)/c +"  "+(m*100)/c;
			String sumu=p;
			context.write(key, new Text(sumu));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		
		Job job=Job.getInstance(c,"Gender wise literacy rate");
		job.setJarByClass(SULITERACYRATE.class);
		job.setMapperClass(MyMapper.class);
		
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem.get(c).delete(new Path(args[1]),true);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);



	}


}
