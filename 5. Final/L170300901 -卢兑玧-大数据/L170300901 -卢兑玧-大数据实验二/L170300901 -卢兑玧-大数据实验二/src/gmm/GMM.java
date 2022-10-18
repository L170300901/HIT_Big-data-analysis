package gmm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class GMM {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int numberMix = 8;
        int featuresNumber = 68;

        Configuration configuration = new Configuration();
        configuration.setInt("gmm.num.mix", numberMix);
        configuration.setInt("gmm.features.number", featuresNumber);

        Job job = Job.getInstance(configuration);
        job.setJobName("GMM Init");
        job.setJarByClass(GMM.class);
        job.setMapperClass(GMMInitMapper.class);
        job.setReducerClass(GMMInitReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        long x = 768 * 1024 * 1024;
        long y = 512 * 1024 * 1024;
        FileInputFormat.setMaxInputSplitSize(job, x);
        FileInputFormat.setMinInputSplitSize(job, y);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_m_0"));

        job.waitForCompletion(true);

        int iteration = 1;
        int MAX_ITERATION = 20;

        while (iteration < MAX_ITERATION) {
            String uri = args[1] + "_m_" + (iteration - 1) + "/part-r-00000";
            Configuration fileConf = new Configuration();
            FileSystem fileSystem = FileSystem.get(URI.create(uri), fileConf);
            Path inputPath = new Path(uri);
            FSDataInputStream inputStream = fileSystem.open(inputPath);
            String[] mu = new String[numberMix];
            String[] sigma = new String[numberMix];
            double[] pi = new double[numberMix];

            for (int i = 0; i < numberMix; i++) {
                String line = inputStream.readLine();
                String[] temp = line.split("\t");
                pi[i] = Double.parseDouble(temp[1]);
                mu[i] = temp[2];
                sigma[i] = temp[3];
            }

            Configuration gmmConf = new Configuration();

            for (int i = 0; i < numberMix; i++) {
                gmmConf.set("gmm.sigma." + i, sigma[i]);
                gmmConf.set("gmm.mu." + i, mu[i]);
                gmmConf.setDouble("gmm.pi." + i, pi[i]);
            }
            gmmConf.setInt("gmm.num.mix", numberMix);
            gmmConf.setInt("gmm.features.number", featuresNumber);

            Job gmm = Job.getInstance(gmmConf);
            gmm.setJarByClass(GMM.class);
            gmm.setJobName("GMM " + iteration);
            gmm.setMapperClass(GMMMapper.class);
            gmm.setReducerClass(GMMReducer.class);
            gmm.setOutputKeyClass(IntWritable.class);
            gmm.setOutputValueClass(Text.class);

            x = 16 * 1024 * 1024;
            FileInputFormat.setMaxInputSplitSize(gmm, x);
            FileInputFormat.setMinInputSplitSize(gmm, x);
            FileInputFormat.addInputPath(gmm, new Path(args[2]));
            FileOutputFormat.setOutputPath(gmm, new Path(args[1] + "_m_" + iteration));
            gmm.waitForCompletion(true);
            iteration++;
        }
    }
}
