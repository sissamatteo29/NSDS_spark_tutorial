package it.polimi.middleware.spark.batch.wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;



public class WordCountModified {

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("WordCount");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        final JavaRDD<String> lines = sc.textFile(filePath + "files/wordcount/in.txt");


        // Q1. For each character, compute the number of words starting with that character
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> initials = words.map(w -> w.substring(0, 1).toLowerCase()).mapToPair(c->new Tuple2<>(c,1));
        JavaPairRDD<String, Integer> counts = initials.reduceByKey((a, b) -> a + b);

        System.out.println("WORDS-----------"+ counts.collect());



        // Q2. For each character, compute the number of lines starting with that character

        JavaPairRDD<String, Integer> lines_initials = lines.map(line -> line.substring(0, 1).toLowerCase()).mapToPair(c->new Tuple2<>(c,1));
        JavaPairRDD<String, Integer> lines_counts = lines_initials.reduceByKey((a, b) -> a + b);

        System.out.println("LINES-----------"+ lines_counts.collect());

        // Q3. Compute the average number of characters in each line
        int total_characters = lines.map(line -> line.replace(" ", "").length()).reduce((a,b) -> a+b);
        int total_lines = lines.map(line -> 1).reduce((a,b)-> a+b);
        System.out.println("average-----------" + (double) total_characters / total_lines);
        System.out.println("lines-----------" + total_lines);

        JavaRDD<Integer> count_characters_line = lines.map(line -> line.replace(" ", "").length());
        System.out.println(count_characters_line.collect());
        JavaPairRDD<Integer, Integer> character_lines = count_characters_line.mapToPair(e -> new Tuple2<>(e, 1));
        System.out.println(character_lines.collect());
        Tuple2<Integer, Integer> sums = character_lines.reduce((e1, e2) -> new Tuple2<>(e1._1 + e2._1, e1._2 + e2._2));

        

        double average = (double) sums._1 / sums._2;
        System.out.println(average);

        sc.close();
    }

}