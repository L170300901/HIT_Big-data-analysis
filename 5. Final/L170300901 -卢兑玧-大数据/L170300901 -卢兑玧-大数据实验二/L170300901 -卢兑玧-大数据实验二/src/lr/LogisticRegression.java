package lr;


import classify.NaiveBayesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class LogisticRegression {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        double learningRate = 0.1;
        int featuresNumber = 18;
        int maxIteration = 100;
        int iteration = 0;
        double[] beta = new double[featuresNumber + 1];
        double epsilon = 1e-4;

        double diff;
        do {
            Configuration configuration = new Configuration();
            diff = 0;
            if (iteration == 0) {
                diff = 1;
                Arrays.fill(beta, 0);
            } else {
                String uri = args[1] + "_m_" + (iteration - 1) + "/part-r-00000";
                Configuration fileConf = new Configuration();
                FileSystem fileSystem = FileSystem.get(URI.create(uri), fileConf);
                Path inputPath = new Path(uri);
                FSDataInputStream inputStream = fileSystem.open(inputPath);
                for (int i = 0; i < featuresNumber + 1; i++) {
                    String line = inputStream.readLine();
                    String[] values = line.split("\t");
                    int index = Integer.parseInt(values[0]);
                    double value = Double.parseDouble(values[1]);
                    diff += Math.abs(beta[index] - value);
                    beta[index] = value;
                }
            }
            for (int i = 0; i < featuresNumber + 1; i++) {
                configuration.setDouble("beta." + i, beta[i]);
            }
            configuration.setInt("features.number", featuresNumber);
            configuration.setDouble("learning.rate", learningRate);

            Job job = Job.getInstance(configuration);
            job.setJarByClass(LogisticRegression.class);
            job.setMapperClass(LogisticRegressionMapper.class);
            job.setReducerClass(LogisticRegressionReducer.class);
            job.setJobName("Logistic Regression");
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "_m_" + iteration));

            job.waitForCompletion(true);

            Job test = Job.getInstance(configuration);
            test.setJarByClass(LogisticRegression.class);
            test.setJobName("LR Test");
            test.setMapperClass(LogisticRegressionTestMapper.class);
            test.setReducerClass(NaiveBayesReducer.class);
            test.setOutputValueClass(IntWritable.class);
            test.setOutputKeyClass(IntWritable.class);
            FileInputFormat.addInputPath(test, new Path(args[0]));
            FileOutputFormat.setOutputPath(test, new Path(args[2] + "_m_" + iteration));

            test.waitForCompletion(true);
            String uri = args[2] + "_m_" + iteration + "/part-r-00000";
            Configuration fileConf = new Configuration();
            FileSystem fileSystem = FileSystem.get(URI.create(uri), fileConf);
            Path inputPath = new Path(uri);
            FSDataInputStream inputStream = fileSystem.open(inputPath);
            int correctNumber = 0;
            int totalNumber = 0;
            for (int i = 0; i < 2; i++) {
                String line = inputStream.readLine();
                if (line == null)
                    continue;
                String [] values = line.split("\t");
                correctNumber += Integer.parseInt(values[1]);
                totalNumber += (Integer.parseInt(values[1]) + Integer.parseInt(values[2]));
            }

            System.out.println(">>>>>>>>>" + iteration);
            System.out.println("Acc: " + ((double) correctNumber / totalNumber));
            System.out.println(Arrays.toString(beta));

            iteration++;
        } while (iteration < maxIteration && diff > epsilon);

    }
}
