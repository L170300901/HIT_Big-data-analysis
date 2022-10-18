package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class KmeansInitMapper extends Mapper<Object, Text, IntWritable, Text> {

    // 随机读取K个数据作为初始的样本中心点

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(1), value);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Random random = new Random(17);
        Configuration configuration = context.getConfiguration();
        int k = configuration.getInt("kmeans.k", 8);
        setup(context);
        int count = 0;
        while (context.nextKeyValue()) {
            if (count < k) {
                if(random.nextInt(10) == 0){
                count++;
                map(context.getCurrentKey(), context.getCurrentValue(), context);
                }
            } else break;
        }
    }
}
