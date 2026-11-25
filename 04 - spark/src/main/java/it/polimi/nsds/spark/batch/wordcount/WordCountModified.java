package it.polimi.nsds.spark.batch.wordcount;

import it.polimi.nsds.spark.common.Consts;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map.Entry;


public class WordCountModified {

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;

        final SparkConf conf = new SparkConf().setMaster(master).setAppName("WordCount");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        final JavaRDD<String> lines = sc.textFile(filePath + "files/wordcount/in.txt");
        final JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());


        // Q1. For each character, compute the number of words starting with that character
        final JavaPairRDD<Character, Integer> wordsInitials = words
                .filter(word -> !word.isEmpty())
                .mapToPair(word -> new Tuple2<>(word.charAt(0), 1))
                .reduceByKey((a, b) -> a + b);
        System.out.println("Q1: " + wordsInitials.collect());


        // Q2. For each character, compute the number of lines starting with that character
        final JavaPairRDD<Character, Integer> linesInitials = lines
                .filter(line -> !line.isEmpty())
                .mapToPair(line -> new Tuple2<>(line.charAt(0), 1))
                .reduceByKey((a, b) -> a + b);
        System.out.println("Q2: " + linesInitials.collect());


        // Q3. Compute the average number of characters in each line
        final int totalCharacters = lines.map(line -> line.length()).reduce((a, b) -> a + b);
        final long numLines = lines.count();
        final double avgLinesLength = (double) totalCharacters / numLines;
        System.out.println("Q3: " + avgLinesLength);


        sc.close();
    }
}