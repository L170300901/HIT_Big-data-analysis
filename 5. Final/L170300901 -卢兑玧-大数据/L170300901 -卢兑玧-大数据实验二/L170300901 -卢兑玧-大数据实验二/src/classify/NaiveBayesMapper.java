package classify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NaiveBayesMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private double g(double x, double mean, double var) {
        return 1.0 / (Math.sqrt(2 * Math.PI * var)) * Math.exp(-((x - mean) * (x - mean)) / (2 * var));
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        double class_0_rate = configuration.getDouble("class_0.rate", 0.5);
        double class_1_rate = configuration.getDouble("class_1.rate", 0.5);
        int features = configuration.getInt("features", 19);
        double[] class_0_mean = new double[features];
        double[] class_0_var = new double[features];
        double[] class_1_mean = new double[features];
        double[] class_1_var = new double[features];

        for (int i = 1; i < features; i++) {
            class_0_mean[i] = configuration.getDouble("class_0." + i + ".mean", 0);
            class_0_var[i] = configuration.getDouble("class_0." + i + ".var", 1);
            class_1_mean[i] = configuration.getDouble("class_1." + i + ".mean", 0);
            class_1_var[i] = configuration.getDouble("class_1." + i + ".var", 1);
        }

        String[] values = value.toString().split("\t");
        String[] params = values[1].split(",");
        double ans0 = class_0_rate;
        double ans1 = class_1_rate;
        for (int i = 0; i < params.length; i++) {
            ans0 *= g(Double.parseDouble(params[i]), class_0_mean[i + 1], class_0_var[i + 1]);
            ans1 *= g(Double.parseDouble(params[i]), class_1_mean[i + 1], class_1_var[i + 1]);
        }
        if (ans0 > ans1)
            context.write(new IntWritable(0), new IntWritable(Integer.parseInt(values[0])));
        else
            context.write(new IntWritable(1), new IntWritable(Integer.parseInt(values[0])));
    }
}
