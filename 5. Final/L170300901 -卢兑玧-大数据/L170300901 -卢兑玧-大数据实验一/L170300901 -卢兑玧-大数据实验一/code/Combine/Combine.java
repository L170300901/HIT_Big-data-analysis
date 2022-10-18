package Step5;

import java.io.IOException;
import java.time.Month;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


public class Combine {
	
	
	private static double MIN = 100000;
	private static double MAX = 0;
	public static String DateConversion(String date) {
		if(date.contains("/")) {
			String newDate = date.replace("/", "-");
			return newDate;
		}
		else if(date.contains(",")) {
			String[] tmp1 = date.split(" ");
			String[] tmp2 = tmp1[1].split(",");
			String month = ""+Month.valueOf(tmp1[0].toUpperCase()).getValue();
			String newDate = tmp2[1]+"-"+month+"-"+tmp2[0];
			return newDate;
		}
		return date;
	}
	public static String TemperatureConversion(String temperature) {
		if(temperature.contains("℉")) {
			String num = temperature.substring(0,temperature.length()-1);
			Double tmp = Double.parseDouble(num);
			Double newNum = (tmp-32)/1.8;
			String newTemperature = newNum+"℃";
			return newTemperature;
		}
		return temperature;
	}
      public static Map<String, Integer> NUM = new HashMap<String, Integer>();
      public static int SUM = 0;
	
	   public static class MapTask extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			double longiLow = 8.1461259, longiHigh = 11.1993265;
			double latiLow = 56.5824856, latiHigh = 57.750511;
			
			String[] words = value.toString().split("\\|");
			Double longitude = Double.parseDouble(words[1]);
			Double latitude = Double.parseDouble(words[2]);
			String reviewdate = words[4];
			String temperature = words[5];
			String birthday = words[8];
			
			String newReviewDate = DateConversion(reviewdate);
			String newTemperature = TemperatureConversion(temperature);
			String newBirthday = DateConversion(birthday);
			
			words[4] = newReviewDate;
			words[5] = newTemperature;
			words[8] = newBirthday;
			
			if(!words[6].contains("?")) {
				Double rate = Double.parseDouble(words[6]);
				if(rate > MAX) {
					MAX = rate;
				}
			   if(rate < MIN) {
				   MIN = rate;
				   }
			}
			
			if(!NUM.containsKey(words[words.length - 2])) {
				NUM.put(words[words.length - 2], 1);
			}
			else {
				NUM.put(words[words.length - 2], NUM.get(words[words.length - 2])+1);
			}
			SUM += 1;
			String newline = "";
			for (int i = 0; i < words.length; i++) {
				if(i < words.length-1) {
					newline = newline+words[i]+"|";
				}
				else {
					newline = newline+words[i];
				}
			}
			if(longitude >= longiLow && longitude <= longiHigh && latitude >= latiLow && latitude <= latiHigh) {
				context.write(new Text(words[words.length-2]), new Text(newline));
			}
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
			for (Text line: values) {
				if(Samples.contains(s)) {
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
				s++;
			}
		}
	}
	
	

	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
      System.setProperty("HADOOP_USER_NAME", "root");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Combine");
		
		job.setJarByClass(Combine.class);
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		
		
		BasicConfigurator.configure();
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/exp1/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/exp1/D_Combine/Step1"));

		
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"优秀":"失败");
		System.exit(job.isSuccessful()?0:1);

	}

}
