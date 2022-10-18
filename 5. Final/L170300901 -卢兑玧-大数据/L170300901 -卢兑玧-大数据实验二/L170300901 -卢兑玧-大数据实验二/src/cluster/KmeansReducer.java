package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;


public class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        int features = configuration.getInt("kmeans.features", 69);
        double[] newCenters = new double[features];
        Arrays.fill(newCenters, 0);
        newCenters[0] = -1;
        int count = 0;
        double wcsse = 0;
        for (Text value : values) {
            String[] row = value.toString().split(";");
            String[] point = row[0].split(",");
            for (int i = 1; i < features; i++) {
                newCenters[i] = (newCenters[i] * count + Double.parseDouble(point[i])) / (count + 1);
            }
            count++;
            wcsse += Double.parseDouble(row[1]);
        }
        StringBuilder outputValue = new StringBuilder();
        for (int i = 0; i < features; i++) {
            outputValue.append(newCenters[i]);
            if (i != features - 1)
                outputValue.append(",");
        }
        outputValue.append("\t").append(wcsse);
        context.write(new IntWritable(key.get()), new Text(outputValue.toString()));
    }
}
