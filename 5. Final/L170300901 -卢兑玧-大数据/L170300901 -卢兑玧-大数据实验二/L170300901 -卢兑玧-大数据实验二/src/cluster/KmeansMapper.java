package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static double getDistance(String[] center, String[] point, int len) {
        double ans = 0;
        for (int i = 1; i < len; i++) {
            double centerCoordinate = Double.parseDouble(center[i]);
            double pointCoordinate = Double.parseDouble(point[i]);

            ans += (centerCoordinate - pointCoordinate) * (centerCoordinate - pointCoordinate);
        }
        return ans;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split(",");
        Configuration configuration = context.getConfiguration();
        int k = configuration.getInt("kmeans.k", 8);
        int features = configuration.getInt("kmeans.features", 69);
        double minDistance = getDistance(row, configuration.get("kmeans.center.0").split(","), features);
        int clusterCenterAssigned = 0;

        for (int i = 1; i < k; i++) {
            String coords = configuration.get("kmeans.center." + i);
            String[] center = coords.split(",");
            double distance = getDistance(row, center, features);
            if (distance < minDistance) {
                clusterCenterAssigned = i;
                minDistance = distance;
            }
        }
        context.write(new IntWritable(clusterCenterAssigned), new Text(value + ";" + minDistance));
    }
}
