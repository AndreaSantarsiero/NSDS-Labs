package it.polimi.nsds.spark.batch.iterative;

import it.polimi.nsds.spark.common.Consts;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


/**
 * Start from a dataset of investments. Each element is a Tuple2(amount_owned, interest_rate).
 * At each iteration the new amount is (amount_owned * (1+interest_rate)).
 *
 * Implement an iterative algorithm that computes the new amount for each investment and stops
 * when the overall amount overcomes 1000.
 */
public class InvestmentSimulator {
    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;
        final double threshold = 1000;

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("InvestmentSimulator");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        final JavaRDD<String> textFile = sc.textFile(filePath + "files/iterative/investment.txt");


        // parsing file rows on Tuple2(amount_owned, interest_rate)
        JavaRDD<Tuple2<Double, Double>> investments = textFile.map(line -> {
            final String[] values = line.trim().split(" ");
            return new Tuple2<>(Double.parseDouble(values[0]), Double.parseDouble(values[1]));
        });
        investments.cache();
        JavaRDD<Tuple2<Double, Double>> oldInvestments = investments;


        // initialization
        int iteration = 0;
        double sum = investments
                .map(a -> a._1)
                .reduce((a, b) -> a + b);


        // iterative simulation
        while(sum <= threshold) {
            investments = investments.map(a -> new Tuple2<Double, Double>(a._1 * (1 + a._2), a._2));
            investments.cache();
            sum = investments
                    .map(a -> a._1)
                    .reduce((a, b) -> a + b);
            oldInvestments.unpersist();
            oldInvestments = investments;
            iteration++;
        }


        System.out.println("Sum: " + sum + " after " + iteration + " iterations");
        sc.close();
    }
}
