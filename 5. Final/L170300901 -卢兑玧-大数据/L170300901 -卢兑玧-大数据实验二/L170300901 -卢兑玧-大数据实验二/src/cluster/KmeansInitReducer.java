package cluster;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class KmeansInitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int clusterCenterId = 0;
        for (Text text : values) {
            context.write(new IntWritable(clusterCenterId++), new Text(text + "\t" + "-1"));
        }
    }
}
