package classify;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class NaiveBayesReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int classIndex = key.get();
        int correctCount = 0;
        int wrongCount = 0;
        for (IntWritable i : values) {
            if (i.get() == classIndex)
                correctCount++;
            else
                wrongCount++;
        }
        context.write(key, new Text(correctCount + "\t" + wrongCount));
    }
}
