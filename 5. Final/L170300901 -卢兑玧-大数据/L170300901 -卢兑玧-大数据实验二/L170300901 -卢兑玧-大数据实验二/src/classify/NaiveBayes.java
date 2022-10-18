package classify;

import jdk.nashorn.internal.objects.NativeRangeError;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;


public class NaiveBayes {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int features = 19;
        int labelIndex = 0;

        Configuration configuration1 = new Configuration();
        configuration1.setInt("label", labelIndex);

        Job job1 = Job.getInstance(configuration1);
        job1.setJarByClass(NaiveBayes.class);
        job1.setJobName("Naive Bayes");

        job1.setMapperClass(NaiveBayesModelInitMapper.class);
        job1.setReducerClass(NaiveBayesModelInitReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        Configuration fileConf = new Configuration();
        String uri = args[1] + "/part-r-00000";
        FileSystem fileSystem = FileSystem.get(URI.create(uri), fileConf);
        Path meanAndVarPath = new Path(uri);
        FSDataInputStream inputStream = fileSystem.open(meanAndVarPath);

        double[] meanClass0 = new double[features];
        double[] meanClass1 = new double[features];
        double[] varClass0 = new double[features];
        double[] varClass1 = new double[features];

        int numberClass0 = 1, numberClass1 = 1;

        for (int i = 0; i < features * 2; i++) {
            String line = inputStream.readLine();
            String[] temp = line.split("\t");
            if (temp.length == 2) {
                if (temp[0].split("_")[0].equals("0")) {
                    numberClass0 = Integer.parseInt(temp[1]);
                } else {
                    numberClass1 = Integer.parseInt(temp[1]);
                }
            } else {
                int classIndex = Integer.parseInt(temp[0].split("_")[0]);
                int paramIndex = Integer.parseInt(temp[0].split("_")[1]);
                if (classIndex == 0) {
                    meanClass0[paramIndex] = Double.parseDouble(temp[1]);
                    varClass0[paramIndex] = Double.parseDouble(temp[2]);
                } else {
                    meanClass1[paramIndex] = Double.parseDouble(temp[1]);
                    varClass1[paramIndex] = Double.parseDouble(temp[2]);
                }
            }
        }
        Configuration configuration2 = new Configuration();
        configuration2.setInt("label", labelIndex);
        configuration2.setInt("features", features);
        configuration2.setDouble("class_0.rate", ((double) numberClass0) / (numberClass0 + numberClass1));
        configuration2.setDouble("class_1.rate", ((double) numberClass1) / (numberClass0 + numberClass1));
        for (int i = 1; i < features; i++) {
            configuration2.setDouble("class_0." + i + ".mean", meanClass0[i]);
            configuration2.setDouble("class_0." + i + ".var", varClass0[i]);
            configuration2.setDouble("class_1." + i + ".mean", meanClass1[i]);
            configuration2.setDouble("class_1." + i + ".var", varClass1[i]);
        }

        Job job2 = Job.getInstance(configuration2);
        job2.setJobName("Naive Bayes");
        job2.setJarByClass(NaiveBayes.class);

        job2.setMapperClass(NaiveBayesMapper.class);
        job2.setReducerClass(NaiveBayesReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true);
    }
}
