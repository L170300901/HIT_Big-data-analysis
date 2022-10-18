package lr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogisticRegressionTestMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        int featuresNumber = configuration.getInt("features.number", 18);
        double[] beta = new double[featuresNumber + 1];
        for (int i = 0; i < featuresNumber + 1; i++) {
            beta[i] = configuration.getDouble("beta." + i, 0);
        }
        double[] x = new double[featuresNumber + 1];
        String[] yx = value.toString().split("\t");
        int y = Integer.parseInt(yx[0]);
        String[] features = yx[1].split(",");
        for (int i = 0; i < featuresNumber; i++) {
            x[i] = Double.parseDouble(features[i]);
        }
        x[featuresNumber] = 1;

        double temp = 0;
        for (int i = 0; i < featuresNumber + 1; i++) {
            temp += beta[i] * x[i];
        }
        double p0 = 1 / (1 + Math.exp(temp));

        if (p0 > 0.5)
            context.write(new IntWritable(0), new IntWritable(y));
        else
            context.write(new IntWritable(1), new IntWritable(y));
    }
}
