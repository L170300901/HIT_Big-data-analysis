package gmm;

import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GMMReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        double sumZ_n_k = 0;

        Configuration configuration = context.getConfiguration();
        int featuresNumber = configuration.getInt("gmm.features.number", 68);

        List<String> xList = new ArrayList<>();
        List<Double> z_n_k = new ArrayList<>();
        for (Text text : values) {
            String[] value = text.toString().split("\t");
            double z = Double.parseDouble(value[0]);
            sumZ_n_k += z;
            z_n_k.add(z);
            xList.add(value[1]);
            count++;
        }

        double pi_k = sumZ_n_k / count;
        double[] mu_k = new double[featuresNumber];
        Arrays.fill(mu_k, 0);
        RealVector mu_k_vector = MatrixUtils.createRealVector(mu_k);
        for (int i = 0; i < count; i++) {
            mu_k_vector = mu_k_vector.add(MatrixUtils.createRealVector(Utils.getMuFromString(xList.get(i)))
                    .mapMultiply(z_n_k.get(i)));
        }
        mu_k = mu_k_vector.mapMultiply(1.0 / sumZ_n_k).toArray();
        double[][] sigma_k = new double[featuresNumber][featuresNumber];
        for (double[] s : sigma_k) {
            Arrays.fill(s, 0);
        }
        RealMatrix sigma_k_matrix = MatrixUtils.createRealMatrix(sigma_k);
        RealMatrix mu_k_matrix = MatrixUtils.createRealMatrix(new double[][]{mu_k});
        for (int i = 0; i < count; i++) {
            RealMatrix xMatrix = MatrixUtils.createRealMatrix(new double[][]{Utils.getMuFromString(xList.get(i))});
            sigma_k_matrix = sigma_k_matrix.add(((xMatrix.subtract(mu_k_matrix)).transpose().multiply((xMatrix.subtract(mu_k_matrix)))).scalarMultiply(z_n_k.get(i)));
        }
        if (Math.abs(new LUDecomposition(sigma_k_matrix).getDeterminant()) < 1e-4) {
            // Avoid singular matrix
            double[] bias = new double[featuresNumber];
            Arrays.fill(bias, 1e-4);
            sigma_k_matrix = sigma_k_matrix.add(MatrixUtils.createRealDiagonalMatrix(bias));
        }
        sigma_k = sigma_k_matrix.scalarMultiply(1.0 / sumZ_n_k).getData();

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(pi_k).append("\t");
        for (int i = 0; i < featuresNumber; i++) {
            stringBuilder.append(mu_k[i]);
            if (i != featuresNumber - 1)
                stringBuilder.append(",");
        }
        stringBuilder.append("\t");
        for (int i = 0; i < featuresNumber; i++) {
            for (int j = 0; j < featuresNumber; j++) {
                stringBuilder.append(sigma_k[i][j]);
                if (j != featuresNumber - 1)
                    stringBuilder.append(",");
            }
            if (i != featuresNumber - 1)
                stringBuilder.append(";");
        }
        context.write(key, new Text(stringBuilder.toString()));
    }
}
