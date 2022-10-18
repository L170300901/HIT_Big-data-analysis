package classify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class ClassifySplitData extends Mapper<Object, Text, IntWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 1; i < values.length; i++) {
            stringBuilder.append(values[i]);
            if (i != values.length - 1)
                stringBuilder.append(",");
        }
        context.write(new IntWritable((int) Double.parseDouble(values[0])), new Text(stringBuilder.toString()));
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        int seed = configuration.getInt("random.seed", 17);
        int trainSet = configuration.getInt("train_or_test", 1);
        // 1 is train set, 0 is test set.
        Random random = new Random(seed);
        setup(context);
        while (context.nextKeyValue()) {
            if (trainSet == 1) {
                if (random.nextInt(10) <= 7) {
                    map(context.getCurrentKey(), context.getCurrentValue(), context);
                }
            } else {
                if (random.nextInt(10) > 7) {
                    map(context.getCurrentKey(), context.getCurrentValue(), context);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.setInt("random.seed", 17);
        configuration.setInt("train_or_test", 0);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ClassifySplitData.class);
        job.setJobName("Classify split data");
        job.setMapperClass(ClassifySplitData.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("classifyInput/"));
        if (configuration.getInt("train_or_test", 1) == 1) {
            FileOutputFormat.setOutputPath(job, new Path("classifyTrainInput/"));
        } else {
            FileOutputFormat.setOutputPath(job, new Path("classifyTestInput/"));
        }
        job.waitForCompletion(true);
    }
}
