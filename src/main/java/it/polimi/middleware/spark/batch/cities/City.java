package it.polimi.middleware.spark.batch.cities;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class City {

    public static void main(String[] args){

        SparkSession spark = SparkSession
            .builder()
            .master("local[4]")
            .appName("cities")
            .getOrCreate();

        // Create a different data schema for each csv
        List<StructField> fieldsRegions = new ArrayList<>();
        fieldsRegions.add(DataTypes.createStructField("city",DataTypes.StringType, true));
        fieldsRegions.add(DataTypes.createStructField("region",DataTypes.StringType, true));
        StructType regionsSchema = DataTypes.createStructType(fieldsRegions);

        final Dataset<Row> regions = spark
            .read()
            .option("header", "false")
            .option("delimiter", ",")
            .schema(regionsSchema)
            .csv("files/cities/regions.csv");

        regions.show();




    }

}
