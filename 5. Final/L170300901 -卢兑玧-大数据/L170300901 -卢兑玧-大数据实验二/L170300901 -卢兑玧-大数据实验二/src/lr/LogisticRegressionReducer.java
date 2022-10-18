package lr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class LogisticRegressionReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        double lambda = configuration.getDouble("lambda", 0.01);
        int index = key.get();
        int count = 0;
        double beta_i = configuration.getDouble("beta." + index, 0);
        double temp = beta_i;

        for (DoubleWritable d : values) {
            temp += d.get();
            count++;
        }

        context.write(key, new DoubleWritable(beta_i + (temp - lambda * beta_i) / count));
    }
}
