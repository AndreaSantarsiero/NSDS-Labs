package it.polimi.nsds.spark.lab1.cities;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.*;


public class Cities {
    public static void main(String[] args) throws TimeoutException, IOException {

        // initialization
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("SparkEval")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");


        // data structures
        final List<StructField> citiesRegionsFields = new ArrayList<>();
        citiesRegionsFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesRegionsFields.add(DataTypes.createStructField("region", DataTypes.StringType, false));
        final StructType citiesRegionsSchema = DataTypes.createStructType(citiesRegionsFields);

        final List<StructField> citiesPopulationFields = new ArrayList<>();
        citiesPopulationFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        citiesPopulationFields.add(DataTypes.createStructField("city", DataTypes.StringType, false));
        citiesPopulationFields.add(DataTypes.createStructField("population", DataTypes.IntegerType, false));
        final StructType citiesPopulationSchema = DataTypes.createStructType(citiesPopulationFields);


        // data sets
        final Dataset<Row> citiesPopulation = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesPopulationSchema)
                .csv(filePath + "files/cities/cities_population.csv");

        final Dataset<Row> citiesRegions = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .schema(citiesRegionsSchema)
                .csv(filePath + "files/cities/cities_regions.csv");



        // 1) total population for each region
        final Dataset<Row> q1 = citiesPopulation
                .join(
                    citiesRegions,
                    citiesPopulation.col("city").equalTo(citiesRegions.col("city")))
                .groupBy("region")
                .sum("population");

        q1.show();


        // 2) compute the number of cities and the population of the most populated city for each region
        final Dataset<Row> q2 = citiesPopulation
                .join(
                        citiesRegions,
                        citiesPopulation.col("city").equalTo(citiesRegions.col("city")))
                .groupBy("region")
                .agg(
                    count("*").as("numCities"),
                    max("population").as("maxPopulation"));

        q2.show();



        // JavaRDD where each element is an integer and represents the population of a city
        JavaRDD<Integer> population = citiesPopulation.toJavaRDD().map(r -> r.getInt(2));
        final int threshold = 100000000;    // 100M threshold

        // 3) Print the evolution of the population in Italy year by year until the total overcomes 100M people
        int year = 0;
        int sum = population.reduce((a, b) -> a + b);
        population.cache();
        JavaRDD<Integer> oldPopulation = population;
        System.out.println("Year: " + year + ", total population: " + sum);

        while(sum <= threshold){
            population = population.map(a -> {
                if(a > 1000) return (int) Math.round(1.01 * a);
                else return (int) Math.round(0.99 * a);
            });
            sum = population.reduce((a, b) -> a + b);
            population.cache();
            oldPopulation.unpersist();
            oldPopulation = population;
            year++;
            System.out.println("Year: " + year + ", total population: " + sum);
        }



        // Bookings: the value represents the city of the booking
        final Dataset<Row> bookings = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 100)
                .load()
                .withColumn("value", col("value").mod(3500));

        // 4) compute the total number of bookings for each region, in a window of 30 seconds, sliding every 5 seconds
        final StreamingQuery q4 = bookings
                .join(citiesPopulation, bookings.col("value").equalTo(citiesPopulation.col("id")))
                .join(citiesRegions, citiesPopulation.col("city").equalTo(citiesRegions.col("city")))
                .groupBy(
                        col("region"),
                        window(col("timestamp"), "30 seconds", "5 seconds"))
                .agg(count("*").as("numBookings"))
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();


        try {
            q4.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }

        spark.close();
    }
}