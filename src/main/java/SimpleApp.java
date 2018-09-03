import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import scala.collection.*;
// $example off:schema_merging$
import java.util.Properties;

// $example on:basic_parquet_example$
import org.apache.spark.api.java.function.MapFunction;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.*;
import org.apache.spark.sql.Dataset;

public class SimpleApp {

  public static void main(String[] args) {
    //String logFile = "hdfs:///tmp/README.md"; // Should be some file on your system
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    //Dataset<String> logData = spark.read().textFile(logFile).cache();

   //Dataset<Row> df = spark.read.format("json");//.option("header", "true").load("hdfs:///tmp/txfile.csv");
   Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", true).csv("hdfs:///tmp/FL_insurance_sample.csv");
   //Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", true).csv("s3://magnify2018/demo-in/FL_insurance_sample.csv");
   System.out.println("========== Print Schema ============");
   df.printSchema();
   System.out.println("========== Print Data ==============");
   df.show();
   System.out.println("========== Print title ==============");
   df.select("county").show();
   
   Dataset<Row> dfFilter=df.filter(df.col("county").equalTo("POLK COUNTY"));
   dfFilter.show();
   dfFilter.coalesce(1)
         .write()
         .option("header", "true")
//         .csv("s3://magnify2018/demo-out");
       .csv("hdfs:///tmp/FL_insurance_filter2");

    //long numAs = logData.filter(s -> s.contains("a")).count();
    //long numBs = logData.filter(s -> s.contains("b")).count();

    //System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    spark.stop();
  }
}

