package Step3;

import java.io.IOException;
import java.time.Month;

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

import Step3.Normalization.MapTask;
import Step3.Normalization.ReduceTask;

public class Conversion {
	
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

	public static class MapTask extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			
			String[] words = value.toString().split("\\|");
			String reviewdate = words[4];
			String temperature = words[5];
			String birthday = words[8];
			
			String newReviewDate = DateConversion(reviewdate);
			String newTemperature = TemperatureConversion(temperature);
			String newBirthday = DateConversion(birthday);
			
			words[4] = newReviewDate;
			words[5] = newTemperature;
			words[8] = newBirthday;
			String newline = "";
			
			for (int i = 0; i < words.length; i++) {
				if(i < words.length-1) {
					newline = newline+words[i]+"|";
				}
				else {
					newline = newline+words[i];
				}
			}
			context.write(new Text(""), new Text(newline));
			
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
		Job job = Job.getInstance(conf, "Conversion");
		
		job.setJarByClass(Conversion.class);
		
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		
		
		BasicConfigurator.configure();
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/exp1/D_ConversionAndNormalization/Normalization/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/exp1/D_ConversionAndNormalization/Conversion"));

		
		
		boolean completion = job.waitForCompletion(true);
		System.out.println(completion?"优秀":"失败");
		System.exit(job.isSuccessful()?0:1);

	}

}
