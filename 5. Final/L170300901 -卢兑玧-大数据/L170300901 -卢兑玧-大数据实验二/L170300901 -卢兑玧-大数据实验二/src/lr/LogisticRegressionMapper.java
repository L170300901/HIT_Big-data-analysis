package lr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Arrays;

public class LogisticRegressionMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        int featuresNumber = configuration.getInt("features.number", 18);
        double learningRate = configuration.getDouble("learning.rate", 0.001);
        double[] beta = new double[featuresNumber + 1];

        for (int i = 0; i < featuresNumber + 1; i++) {
            double beta_i = configuration.getDouble("beta." + i, 0);
            beta[i] = beta_i;
        }

        String[] yx = value.toString().split("\t");
        String[] features = yx[1].split(",");

        int y = Integer.parseInt(yx[0]);
        double[] x = new double[featuresNumber + 1];
        for (int i = 0; i < featuresNumber; i++)
            x[i] = Double.parseDouble(features[i]);
        x[featuresNumber] = 1;

        double[] ans = Arrays.copyOf(x, featuresNumber + 1);

        double temp = 0;
        for (int i = 0; i < featuresNumber + 1; i++) {
            temp += beta[i] * x[i];
        }
        double p1 = 1.0 - 1 / (1 + Math.exp(temp));

        for (int i = 0; i < featuresNumber + 1; i++) {
            ans[i] *= (y - p1);
            context.write(new IntWritable(i), new DoubleWritable(learningRate * ans[i]));
        }
    }
}
