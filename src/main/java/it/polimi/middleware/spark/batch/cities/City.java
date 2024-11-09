package it.polimi.middleware.spark.batch.cities;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
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

        // Schema for regions
        List<StructField> fieldsRegions = new ArrayList<>();
        fieldsRegions.add(DataTypes.createStructField("city",DataTypes.StringType, true));
        fieldsRegions.add(DataTypes.createStructField("region",DataTypes.StringType, true));
        StructType regionsSchema = DataTypes.createStructType(fieldsRegions);

        // Schema for population
        List<StructField> fieldsPopulation = new ArrayList<>();
        fieldsPopulation.add(DataTypes.createStructField("id",DataTypes.IntegerType, false));
        fieldsPopulation.add(DataTypes.createStructField("city",DataTypes.StringType, true));
        fieldsPopulation.add(DataTypes.createStructField("population",DataTypes.IntegerType, true));
        StructType populationSchema = DataTypes.createStructType(fieldsPopulation);

        final Dataset<Row> regions = spark
            .read()
            .option("header", "false")
            .option("delimiter", ",")
            .schema(regionsSchema)
            .csv("files/cities/regions.csv");   
        
        System.out.println("################ regions #####################");
        regions.show();
        
        
        final Dataset<Row> populations = spark
            .read()
            .option("header", "false")
            .option("delimiter", ",")
            .schema(populationSchema)
            .csv("files/cities/population.csv");
        
        System.out.println("################ populations #####################");
        populations.show();
        
        // Join by city
        final Dataset<Row> joinRP = regions
        .join(populations, regions.col("city").equalTo(populations.col("city")), "full_outer")
        .drop(regions.col("city")).drop(populations.col("id")).drop(regions.col("city"));
        
        System.out.println("################ joinRP #####################");
        joinRP.show();
        
        // Count population per region
        final Dataset<Row> countPop = joinRP
            .groupBy("region").agg(functions.sum("population").alias("sum_pop"));
        
        // countPop.show();
        
        
        // Count cities per region, extract max population
        Dataset<Row> countCities = joinRP
            .groupBy("region")
            .agg(functions.count("*").as("#_cities")
            ,functions.max("population").as("#max_city_pop"));

        countCities.show();
        
        Dataset<Row> popAlias = populations.withColumnRenamed("city", "city2");

        // Perform the join using qualified column references and select specific columns
        Dataset<Row> cityWithPop = countCities
        .join(popAlias, popAlias.col("population").equalTo(countCities.col("#max_city_pop")), "left")
        .select(countCities.col("region"), 
                countCities.col("#_cities"), 
                countCities.col("#max_city_pop"),
                popAlias.col("city2"));
        cityWithPop.show();









    }
}
