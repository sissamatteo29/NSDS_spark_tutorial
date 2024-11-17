package it.polimi.middleware.spark;

import org.apache.spark.sql.SparkSession;

public class Evaluation_lab {
    private static final boolean useCache = true;


    private static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = useCache ? "Cache" : "NoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("SparkEval")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");






    }
}
