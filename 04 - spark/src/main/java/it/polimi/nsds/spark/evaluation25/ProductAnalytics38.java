package it.polimi.nsds.spark.evaluation25;

/*
 * Group 38
 * Members: Andrea Santarsiero, Tanish Kumar, Mostafa Karimpoorfard
 */

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;
import static org.apache.spark.sql.functions.*;



public class ProductAnalytics38 {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local[8]")
                .appName("ProductAnalytics")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");


        // define data structures
        final List<StructField> productsFields = new ArrayList<>();
        productsFields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
        productsFields.add(DataTypes.createStructField("category", DataTypes.StringType, false));
        productsFields.add(DataTypes.createStructField("price", DataTypes.DoubleType, false));
        final StructType productsSchema = DataTypes.createStructType(productsFields);

        final List<StructField> historicalPurchasesFields = new ArrayList<>();
        historicalPurchasesFields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
        historicalPurchasesFields.add(DataTypes.createStructField("quantity", DataTypes.IntegerType, false));
        final StructType historicalPurchasesSchema = DataTypes.createStructType(historicalPurchasesFields);


        // Load static datasets
        Dataset<Row> products = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .schema(productsSchema)
                .csv("files/evaluation25/products.csv");

        Dataset<Row> historicalPurchases = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .schema(historicalPurchasesSchema)
                .csv("files/evaluation25/historicalPurchases.csv");


        // Streaming source: purchasesStream
        Dataset<Row> purchasesStream = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 5)
                .load();


        // Map the "value" counter to product IDs and quantities
        purchasesStream = purchasesStream
                .select(
                        col("timestamp"),
                        when(col("value").mod(10).lt(2), lit("P0582"))
                                .when(col("value").mod(10).lt(3), lit("P0112"))
                                .when(col("value").mod(10).lt(5), lit("P0536"))
                                .when(col("value").mod(10).equalTo(6), lit("P0742"))
                                .when(col("value").mod(10).equalTo(8), lit("P0027"))
                                .otherwise(lit("P00" + Math.random() % 100))
                                .alias("Product"),
                        (col("value").mod(5).plus(1)).alias("Quantity")       // 1–5 units
                );



        // ==========================================
        // Q1: Top 5 most purchased products
        // ==========================================
        Dataset<Row> topProducts = null;



        // ==========================================
        // Q2: Revenue per (Category, TopProduct)
        // ==========================================
        Dataset<Row> revenue = null;



        // ==========================================
        // Q3: Streaming window for top products
        // ==========================================
        Dataset<Row> windowed = null;



        // ==========================================
        // Q4: Difference historical vs streaming
        // ==========================================
        Dataset<Row> diff = null;

    }
}
