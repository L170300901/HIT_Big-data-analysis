package gmm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GMMInitReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        int k = configuration.getInt("gmm.num.mix", 8);
        int featuresNumber = configuration.getInt("gmm.features.number", 68);
        double[][] sigma = new double[featuresNumber][featuresNumber];
        for (int i = 0; i < featuresNumber; i++) {
            for (int j = 0; j < featuresNumber; j++) {
                if (i == j)
                    sigma[i][j] = 0.1;
                else
                    sigma[i][j] = 0;
            }
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < featuresNumber; i++) {
            for (int j = 0; j < featuresNumber; j++) {
                sb.append(sigma[i][j]);
                if (j != featuresNumber - 1)
                    sb.append(",");
            }
            if (i != featuresNumber - 1)
                sb.append(";");
        }
        int count = 0;
        for (Text text : values) {
            String mu = text.toString();
            mu = mu.substring(mu.indexOf(",") + 1);
            context.write(new IntWritable(count++), new Text(1.0 / k + "\t" + mu + "\t" + sb.toString()));
        }
    }
}
