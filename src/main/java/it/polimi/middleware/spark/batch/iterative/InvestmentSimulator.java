package it.polimi.middleware.spark.batch.iterative;

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
        final String master = args.length > 0 ? args[0] : "local[1]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final double threshold = 1000;

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("InvestmentSimulator");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        final JavaRDD<String> linesFromTextFile = sc.textFile(filePath + "files/iterative/investment.txt");


        /***************************** SOLUTION **************************************/
        /* 
        The text file contains a list of items with this format
            
                3 0.01
                4 0.015
                2 0.05  
        
        First, let's turn the textual lines of the file, into a JavaRDD<Tuple2<Double, Double>> containing the numeric values.
        */
        JavaRDD<Tuple2<String, String>> textualInvestments = linesFromTextFile.map(

            line -> new Tuple2(line.split(" ")[0], line.split(" ")[1])

        );

        JavaRDD<Tuple2<Double, Double>> investments = textualInvestments.map(

            elem -> new Tuple2(Double.parseDouble(elem._1()), Double.parseDouble(elem._2()))

        );


        /*
         * Now create a loop to update all the investments in the JavaRDD investments structure.
         * At each iteration sum all the investments to verify whether to stop.
         */

        int iteration = 0;
        double sum = 0;

        while (sum < threshold) {
            // Update current values
            investments.map(
                elem -> new Tuple2<>(elem._1() * (1 + elem._2()), elem._2())
            );

            // Compute total sum of current investments
            // Notice that map requires all inputs and outputs to be of the same type. mapToXXX makes you choose the target type
            sum = investments.mapToDouble(inv -> inv._1()).sum();
        }

        System.out.println("Sum: " + sum + " after " + iteration + " iterations");
        sc.close();
    }

}
