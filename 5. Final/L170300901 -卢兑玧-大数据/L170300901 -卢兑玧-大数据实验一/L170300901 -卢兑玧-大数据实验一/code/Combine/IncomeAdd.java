package Step5;

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


public class IncomeAdd {
	public static class MapTask extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			
			String[] words = value.toString().split("\\|");
			String line = value.toString();
			String KEY = words[9]+"|"+words[10];
			context.write(new Text(KEY), new Text(line));
			
		}

	}
	
	public static class ReduceTask extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> LostNum = new ArrayList<String>();
			double sum = 0;
			int tmp = 0;
			for (Text line : values) {
				String[] words = line.toString().split("\\|");
				if(words[11].contains("?")) {
					LostNum.add(line.toString());
				}
				else {
					sum += Double.parseDouble(words[11]);
					tmp += 1;
					context.write(null, new Text(line));
				}
			}
			String income = "";
			if(tmp == 0) {
				income = "0.0";
			}
			if(tmp != 0) {
				Double inco = sum/tmp;
				income = inco.toString();
			}
			for(String line : LostNum) {
				String[] words = line.toString().split("\\|");
				words[11] = income;
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
		}
	}
	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
      System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "IncomeAdd");
		
		job.setJarByClass(IncomeAdd.class);
		
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		
		
		BasicConfigurator.configure();
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/exp1/D_Combine/Step1/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/exp1/D_Combine/Step2"));

		
		
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"优秀":"失败");
		System.exit(job.isSuccessful()?0:1);

	}
}
