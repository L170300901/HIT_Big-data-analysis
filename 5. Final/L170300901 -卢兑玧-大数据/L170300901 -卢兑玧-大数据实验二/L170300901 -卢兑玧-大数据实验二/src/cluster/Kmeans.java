package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class Kmeans {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int features = 69;
        int k = 8;
        int maxIteration = 100;
        double epsilon = 1e-6;

        int iteration = 0;
        String[] centerCoordinates = new String[k];


        Configuration configuration = new Configuration();
        configuration.setInt("kmeans.k", k);
        configuration.setInt("kmeans.features", features);
        Job job1 = Job.getInstance(configuration);

        job1.setJarByClass(Kmeans.class);
        job1.setJobName("Classic K-means Init");

        job1.setMapperClass(KmeansInitMapper.class);
        job1.setReducerClass(KmeansInitReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        double wcsse = 0;
        double wcssePrevious = -1;

        do {
            System.out.println("Iteration " + iteration);

            if (iteration == 0) {
                long x = 768 * 1024 * 1024;
                long y = 512 * 1024 * 1024;
                long defaultX = FileInputFormat.getMaxSplitSize(job1);
                long defaultY = FileInputFormat.getMinSplitSize(job1);
                FileInputFormat.setMaxInputSplitSize(job1, x);
                FileInputFormat.setMinInputSplitSize(job1, y);
                FileInputFormat.addInputPath(job1, new Path(args[0]));
                FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_m_" + iteration));
                job1.waitForCompletion(true);
                FileInputFormat.setMaxInputSplitSize(job1, defaultX);
                FileInputFormat.setMinInputSplitSize(job1, defaultY);
            } else {
                wcssePrevious = wcsse;
                Configuration conf = new Configuration();
                conf.setInt("kmeans.k", k);
                conf.setInt("kmeans.features", features);

                String uri = args[1] + "_m_" + (iteration - 1) + "/part-r-00000";
                Configuration fileConf = new Configuration();
                FileSystem fileSystem = FileSystem.get(URI.create(uri), fileConf);
                Path inputPath = new Path(uri);
                FSDataInputStream inputStream = fileSystem.open(inputPath);
                wcsse = 0;

                for (int i = 0; i < k; i++) {
                    try {
                        String line = inputStream.readLine();
                        System.out.println(line);
                        String[] temp = line.split("\t");
                        int centerId = Integer.parseInt(temp[0]);
                        centerCoordinates[centerId] = temp[1];
                        wcsse += Double.parseDouble(temp[2]);
                    } catch (EditLogInputException e) {
                        e.printStackTrace();
                    }
                }
                for (int i = 0; i < k; i++)
                    conf.set("kmeans.center." + i, centerCoordinates[i]);

                Job job2 = Job.getInstance(conf);
                job2.setJarByClass(Kmeans.class);
                job2.setMapperClass(KmeansMapper.class);
                job2.setReducerClass(KmeansReducer.class);
                job2.setJobName("Classic K-Means Iteration " + iteration);
                job2.setOutputKeyClass(IntWritable.class);
                job2.setOutputValueClass(Text.class);
                System.out.println(uri);
                System.out.println(">>>>>>>>> WCSSE = " + wcsse);
                System.out.println(">>>>>>>>> WCSSE change = " + Math.abs(wcsse - wcssePrevious) / Math.abs(wcssePrevious));

                long x = 32 * 1024 * 1024;
                job2.setNumReduceTasks(1);
                FileInputFormat.setMaxInputSplitSize(job2, x);
                FileInputFormat.setMinInputSplitSize(job2, x);
                FileInputFormat.addInputPath(job2, new Path(args[0]));
                FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_m_" + iteration));

                job2.waitForCompletion(true);
            }
            iteration++;
        } while (Math.abs(wcsse - wcssePrevious) / Math.abs(wcssePrevious) > epsilon && iteration < maxIteration);
    }


}
