package classify;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class NaiveBayesModelInitMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        String[] params = values[1].split(",");
//        Configuration configuration = context.getConfiguration();
//        int label = configuration.getInt("label", 0);
        int label = Integer.parseInt(values[0]);
        context.write(new Text(label + "_" + 0), new DoubleWritable(label));
        for (int i = 0; i < params.length; i++) {
            context.write(new Text(label + "_" + (i + 1)), new DoubleWritable(Double.parseDouble(params[i])));
        }
    }
}
