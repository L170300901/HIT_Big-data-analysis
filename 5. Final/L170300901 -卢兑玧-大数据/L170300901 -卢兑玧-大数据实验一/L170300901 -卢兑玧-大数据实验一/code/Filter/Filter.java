package Step2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


public class Filter {


	
	public static class MapTask extends Mapper<LongWritable, Text, Text, Text>{
		double longiLow = 8.1461259, longiHigh = 11.1993265;
		double latiLow = 56.5824856, latiHigh = 57.750511;
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			
			String[] words = value.toString().split("\\|");
			String line = value.toString();
			Double longitude = Double.parseDouble(words[1]);
			Double latitude = Double.parseDouble(words[2]);
			
			if(longitude >= longiLow && longitude <= longiHigh && latitude >= latiLow && latitude <= latiHigh) {
				context.write(new Text(""), new Text(line));
			}
			
		}

	}
	
	public static class ReduceTask extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text line : values) {
				context.write(null, new Text(line));
			}
		}
	}
	

	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
      System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Filter");
		
		job.setJarByClass(Filter.class);
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		
		
		BasicConfigurator.configure();
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/exp1/D_Sample/D_Sample"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/exp1/D_Filter"));

		
		
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"优秀":"失败");
		System.exit(job.isSuccessful()?0:1);

	}

}
