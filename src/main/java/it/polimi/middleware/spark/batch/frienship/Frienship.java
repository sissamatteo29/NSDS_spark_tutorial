
package it.polimi.middleware.spark.batch.frienship;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class Frienship {

    public static void main(String[] args) {
        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";

        final SparkSession spark = SparkSession
            .builder()
            .master(master)
            .appName("friendship")
            .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        

        // Create the structure for data
        final List<StructField> schemaFields = new ArrayList<>();
        schemaFields.add(DataTypes.createStructField("person", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("friend", DataTypes.StringType, true));
        final StructType schema = DataTypes.createStructType(schemaFields);

        final Dataset<Row> friends = spark
            .read()
            .option("header", "false")
            .option("delimiter", ",")
            .schema(schema)
            .csv(filePath + "files/friendship/friends.csv");

        // Compute the first self join between the table and itself
        //final Dataset<Row> first = friends.join(friends,  )
        





    }
}