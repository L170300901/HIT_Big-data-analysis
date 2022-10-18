package classify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class NaiveBayesModelInitReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        int labelIndex = configuration.getInt("label", 0);
        String keyString = key.toString();
        int index = Integer.parseInt(keyString.split("_")[1]);
        if (index == labelIndex) {
            int count = 0;
            for (DoubleWritable d : values)
                count++;
            context.write(key, new Text(count + ""));
        } else {
            double sumSqr = 0;
            double sum = 0;
            int count = 0;
            for (DoubleWritable d : values) {
                double dd = d.get();
                sum += dd;
                sumSqr += (dd * dd);
                count++;
            }
            double mean = sum / count;
            double var = (sumSqr - (sum * sum) / count) / count;
            context.write(key, new Text(mean + "\t" + var));
        }
    }
}
