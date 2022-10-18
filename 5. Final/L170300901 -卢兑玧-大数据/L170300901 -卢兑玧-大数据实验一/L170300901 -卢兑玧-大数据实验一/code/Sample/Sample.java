package Step1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Sample {
	
	public static Map<String, Integer> NUM = new HashMap<String, Integer>();
    public static int SUM = 0;
	
	public static class MapTask extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			
			
			String[] words = value.toString().split("\\|");
			String line = value.toString();
			if(!NUM.containsKey(words[words.length - 2])) {
				NUM.put(words[words.length - 2], 1);
			}
			else {
				NUM.put(words[words.length - 2], NUM.get(words[words.length - 2])+1);
			}
			SUM += 1;
			context.write(new Text(words[words.length - 2]), new Text(line));
			
		}

	}
	
	public static class ReduceTask extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String career = key.toString();
			int SampleSum = (int) Math.round(10000.0 * NUM.get(career)/SUM);
			int tmp = (int)Math.floor(1.0 * NUM.get(career)/SampleSum);
			List<Integer> Samples = new ArrayList<Integer>();
			int s = 0;
			while(SampleSum > 0) {
				Samples.add(s);
				s += tmp;
				SampleSum--;
			}
			s = 0;
			for (Text data: values) {
				if(Samples.contains(s)) {
					context.write(null, new Text(data));
				}
				s++;
			}
		}
	}
	
	

	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
      System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sample");
		
		job.setJarByClass(Sample.class);
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		
		BasicConfigurator.configure();

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/exp1/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/exp1/D_Sample"));

		
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"优秀":"失败");
		System.exit(job.isSuccessful()?0:1);

	}

}
