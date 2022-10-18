package lr;

import classify.NaiveBayesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class LogisticRegressionTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int times = 74;
        int featuresNumber = 18;
        double[] beta = new double[featuresNumber + 1];
        for (int i = 0; i <= times; i++) {
            String uri = args[1] + "_m_" + i + "/part-r-00000";
            Configuration fileConf = new Configuration();
            FileSystem fileSystem = FileSystem.get(URI.create(uri), fileConf);
            Path inputPath = new Path(uri);
            FSDataInputStream inputStream = fileSystem.open(inputPath);
            for (int j = 0; j < featuresNumber + 1; j++) {
                String line = inputStream.readLine();
                String[] values = line.split("\t");
                int index = Integer.parseInt(values[0]);
                double value = Double.parseDouble(values[1]);
                beta[index] = value;
            }
            Configuration configuration = new Configuration();
            for (int j = 0; j < featuresNumber + 1; j++) {
                configuration.setDouble("beta." + j, beta[j]);
            }
            Job test = Job.getInstance(configuration);
            test.setJarByClass(LogisticRegression.class);
            test.setJobName("LR Test");
            test.setMapperClass(LogisticRegressionTestMapper.class);
            test.setReducerClass(NaiveBayesReducer.class);
            test.setOutputValueClass(IntWritable.class);
            test.setOutputKeyClass(IntWritable.class);
            FileInputFormat.addInputPath(test, new Path(args[0]));
            FileOutputFormat.setOutputPath(test, new Path(args[2] + "_m_" + i));

            test.waitForCompletion(true);
            uri = args[2] + "_m_" + i + "/part-r-00000";
            fileConf = new Configuration();
            fileSystem = FileSystem.get(URI.create(uri), fileConf);
            inputPath = new Path(uri);
            inputStream = fileSystem.open(inputPath);
            int correctNumber = 0;
            int totalNumber = 0;
            for (int j = 0; j < 2; j++) {
                String line = inputStream.readLine();
                if (line == null)
                    continue;
                String [] values = line.split("\t");
                correctNumber += Integer.parseInt(values[1]);
                totalNumber += (Integer.parseInt(values[1]) + Integer.parseInt(values[2]));
            }
            System.out.println(">>>>>>>>>" + i);
            System.out.println("Acc: " + ((double) correctNumber / totalNumber));
            System.out.println(Arrays.toString(beta));

        }
    }
}
