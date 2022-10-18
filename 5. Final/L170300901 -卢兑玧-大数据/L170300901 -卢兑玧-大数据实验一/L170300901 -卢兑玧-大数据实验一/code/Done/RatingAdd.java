package Step4;

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



public class RatingAdd {
	
	public static class MapTask extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			
			String[] words = value.toString().split("\\|");
			String line = value.toString();
			int longitude = (int) Math.round(Double.parseDouble(words[1]));
			int latitude = (int) Math.round(Double.parseDouble(words[2]));
			int altitude = (int) Math.round(Double.parseDouble(words[3]));
			String KEY = "" + longitude + "|" + latitude + "|" + altitude;
			context.write(new Text(KEY), new Text(line));
			
		}
	}
	
	public static class ReduceTask extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			List<String> LostRate = new ArrayList<String>();
		    List<String> CompleteRate = new ArrayList<String>();
			double sum = 0;
			int tmp = 0;
			for (Text line : values) {
				String[] words = line.toString().split("\\|");
				Double income = Double.parseDouble(words[11]);
				if(words[6].contains("?")) {
					LostRate.add(line.toString());
				}
				else {
					CompleteRate.add(line.toString());
					context.write(null, new Text(line));
				}
			}
			for (String lost : LostRate) {
				sum = 0;
				tmp = 0;
				String[] lostWords = lost.split("\\|");
				Double LostIncome = Double.parseDouble(lostWords[11]);
				for(String complete : CompleteRate) {
					String[] completeWords = complete.split("\\|");
					Double CompleteIncome = Double.parseDouble(completeWords[11]);
					
					if(Math.abs(LostIncome - CompleteIncome) < 200.0) {
						sum += Double.parseDouble(completeWords[6]);
						tmp += 1;
					}
				}
				
				if(tmp == 0) {
					lostWords[6] = "0.0";
				}
				if(tmp != 0) {
					lostWords[6] = "" + sum/tmp;
				}
				
				String newline = "";
				for (int i = 0; i < lostWords.length; i++) {
					if(i < lostWords.length-1) {
						newline = newline+lostWords[i]+"|";
					}
					else {
						newline = newline+lostWords[i];
					}
				}
				context.write(null, new Text(newline));
			}
		}
	}

	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
      System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "RatingAdd");
		
		job.setJarByClass(RatingAdd.class);
		
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		
		
		BasicConfigurator.configure();
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/exp1/D_Done/IncomeAdd/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/exp1/D_Done/RatingAdd"));

		
		
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"优秀":"失败");
		System.exit(job.isSuccessful()?0:1);

	}

}
