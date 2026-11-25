package it.polimi.nsds.spark.batch.bank;

import static org.apache.spark.sql.functions.*;
import java.util.ArrayList;
import java.util.List;
import it.polimi.nsds.spark.common.Consts;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * Bank example
 *
 * Input: csv files with list of deposits and withdrawals, having the following
 * schema ("person: String, account: String, amount: Int)
 *
 * Queries
 * Q1. Print the total amount of withdrawals for each person.
 * Q2. Print the person with the maximum total amount of withdrawals
 * Q3. Print all the accounts with a negative balance
 */
public class Bank {
    private static final boolean useCache = true;

    public static void main(String[] args) throws Exception {
        final String master = args.length > 0 ? args[0] : Consts.MASTER_ADDR_DEFAULT;
        final String filePath = args.length > 1 ? args[1] : Consts.FILE_PATH_DEFAULT;
        final String appName = useCache ? "BankWithCache" : "BankNoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");


        // data structures
        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("person", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("account", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("amount", DataTypes.IntegerType, true));
        final StructType mySchema = DataTypes.createStructType(mySchemaFields);


        // data sets
        final Dataset<Row> deposits = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(mySchema)
                .csv(filePath + "files/bank/deposits.csv");

        final Dataset<Row> withdrawals = spark
                .read()
                .option("header", "false")
                .option("delimiter", ",")
                .schema(mySchema)
                .csv(filePath + "files/bank/withdrawals.csv");



        // Q1. Total amount of withdrawals for each person
        final Dataset<Row> q1 = withdrawals
                .groupBy(col("person"))
                .agg(sum("amount").as("totalWithdrawals"));

        if (useCache) q1.cache();
        q1.show();


        // Q2. Person with the maximum total amount of withdrawals
        final Dataset<Row> maxTotal = q1.agg(max("totalWithdrawals").as("totalWithdrawals"));
        final Dataset<Row> q2 = q1.join(maxTotal, "totalWithdrawals");
        q2.show();


        // Q3 Accounts with negative balance
        final Dataset<Row> q3 = deposits
                .select(col("account"), col("amount"))
                .union(withdrawals.select(col("account"), col("amount").multiply(-1).as("amount")))
                .groupBy(col("account"))
                .agg(sum("amount").as("balance"))
                .filter(col("balance").lt(0));
        q3.show();


        spark.close();
    }
}