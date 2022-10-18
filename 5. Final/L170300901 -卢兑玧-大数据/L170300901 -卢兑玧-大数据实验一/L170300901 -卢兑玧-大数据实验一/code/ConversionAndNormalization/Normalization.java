package Step3;

import java.io.IOException;

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


public class Normalization {
	
	private static double MIN = 100000;
	private static double MAX = 0;
	
	public static class MapTask extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			
			String[] words = value.toString().split("\\|");
			String line = value.toString();
			
			
			if(!words[6].contains("?")) {
				Double rate = Double.parseDouble(words[6]);
				if(rate > MAX) {
					MAX = rate;
				}
			   if(rate < MIN) {
				   MIN = rate;
				   }
			}
			context.write(new Text(""), new Text(line));
			
		}

	}
	
	public static class ReduceTask extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text line : values) {
				String[] words = line.toString().split("\\|");
				
				
				if(!words[6].contains("?")) {
					Double rate = Double.parseDouble(words[6]);
					Double newrate = (rate-MIN)/(MAX-MIN);
					words[6] = newrate.toString();
					String newline = "";
					for (int i = 0; i < words.length; i++) {
						if(i < words.length-1) {
							newline = newline+words[i]+"|";
						}
						else {
							newline = newline+words[i];
						}
					}
					context.write(null, new Text(newline));
				}
				else {
					context.write(null, new Text(line));
				}
			}
		}
	}
	

	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
      System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Normalication");
		
		job.setJarByClass(Normalization.class);
		
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		
		
		BasicConfigurator.configure();
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/exp1/D_Filter/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/exp1/D_ConversionAndNormalization/Normalization"));

		
		
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"优秀":"失败");
		System.exit(job.isSuccessful()?0:1);

	}

}
