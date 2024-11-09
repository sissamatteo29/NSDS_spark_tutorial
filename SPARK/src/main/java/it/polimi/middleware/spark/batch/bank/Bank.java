package it.polimi.middleware.spark.batch.bank;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
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

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = useCache ? "BankWithCache" : "BankNoCache";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("person", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("account", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("amount", DataTypes.IntegerType, true));
        final StructType mySchema = DataTypes.createStructType(mySchemaFields);

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


        /* 
         * Q1. Total amount of withdrawals for each person  
         * 
        */
        
        final Dataset<Row> totWit = withdrawals.groupBy("person").agg(functions.sum("amount").alias("total_withdrawals"));
        totWit.show();
        
        /*
         * Q2. Person with the maximum total amount of withdrawals
         * Nested queries: need to combine multiple DataSets
         */
        final Dataset<Row> countWithdrawalsPerPerson = withdrawals.groupBy("person").agg(functions.count("person").alias("number_of_withdrawals"));
        countWithdrawalsPerPerson.show();

        final Dataset<Row> maxWithdrawals = countWithdrawalsPerPerson.agg(functions.max("number_of_withdrawals").alias("max_withdrawals"));
        maxWithdrawals.show();
         
        final Dataset<Row> personWithMaxWithdrawals = countWithdrawalsPerPerson.join(maxWithdrawals, countWithdrawalsPerPerson.col("number_of_withdrawals").equalTo(maxWithdrawals.col("max_withdrawals")));
        personWithMaxWithdrawals.show();

        /*
         * Q3 Accounts with negative balances
        */ 

        // Compute for each person the total amount deposited
        final Dataset<Row> totDep = deposits
                .groupBy("person").agg(functions.sum("amount").alias("total_deposits"));
        
        totDep.show();

        // Compute the join table between the total for deposits and the total for withdrawals, based on person 
        final Dataset<Row> joinDepWit = totDep.
                join(totWit, totDep.col("person").equalTo(totWit.col("person")));
        
        joinDepWit.show();

        // Add a new column to the previous table with the difference between the totals
        final Dataset<Row> diff = joinDepWit
                .withColumn("difference", joinDepWit.col("total_deposits").minus(joinDepWit.col("total_withdrawals")));
       
        diff.show();

        // Filter only the ones with positive balance
        final Dataset<Row> positives = diff
                .filter(diff.col("difference").gt(0));
        
        
        positives.show();


        /*
         * Q4 Print all accounts in descending order of balance 
         */

        // Start from the "diff" Dataset and select only relevant columns 
        // (notice that after a join all columns, also duplicated, are kept. To remove them it is necessary to select and use the
        //  specific name of the previous table, otherwise ambiguous).
        final Dataset<Row> balances = diff
                .select(totDep.col("person"), diff.col("difference"));

        balances.show();

        // Order based on the balance 
        final Dataset<Row> ordered = balances
                .orderBy(functions.asc("difference"));
        
        ordered.show();
        


        spark.close();

    }
}