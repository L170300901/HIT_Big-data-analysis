package gmm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class GMMMapper extends Mapper<Object, Text, IntWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        double[][] mu;
        double[][][] sigma;
        double[] pi;
        Configuration configuration = context.getConfiguration();

        int k = configuration.getInt("gmm.num.mix", 8);
        int featuresNumber = configuration.getInt("gmm.features.number", 68);

        mu = new double[k][featuresNumber];
        sigma = new double[k][featuresNumber][featuresNumber];
        pi = new double[k];
        for (int i = 0; i < k; i++) {
            String sigmaString = configuration.get("gmm.sigma." + i);
            String muString = configuration.get("gmm.mu." + i);
            mu[i] = Utils.getMuFromString(muString);
            sigma[i] = Utils.getSigmaFromString(sigmaString);
            pi[i] = configuration.getDouble("gmm.pi." + i, 1.0 / k);
        }

        String[] values = value.toString().split(",");
        StringBuilder stringBuilder = new StringBuilder();

        int index = Integer.parseInt(values[0]);
        double[] x = new double[featuresNumber];
        for (int i = 1; i < values.length; i++) {
            x[i - 1] = Integer.parseInt(values[i]);
            stringBuilder.append(x[i - 1]);
            if (i != values.length - 1)
                stringBuilder.append(",");
        }

        String xString = stringBuilder.toString();

        double sumZ_n_k = 0;
        double[] z_n_k = new double[k];

        for (int i = 0; i < k; i++) {
            z_n_k[i] = Utils.gaussian(x, mu[i], sigma[i]) * pi[i];
            if(Double.isNaN(z_n_k[i]))
                z_n_k[i] = 0.0;
            sumZ_n_k += z_n_k[i];
        }

        for (int i = 0; i < k; i++) {
            double p = z_n_k[i] / sumZ_n_k;
            if (Double.isNaN(p))
                p = 0.0;
            context.write(new IntWritable(i), new Text(p + "\t" + xString));
        }
    }
}
