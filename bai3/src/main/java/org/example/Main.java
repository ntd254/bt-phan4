package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

import javax.xml.crypto.Data;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        createParquetFile();
    }

    public static void createParquetFile() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("spark")
                .getOrCreate();
        JavaRDD<Log> logRDD = sparkSession.read()
                .textFile("hdfs://localhost:9000/user/dat254/*.dat")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("\t");
                    Timestamp timeCreate = Timestamp.valueOf(LocalDateTime.parse(parts[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    Timestamp cookieCreate = Timestamp.valueOf(LocalDateTime.parse(parts[1], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    int browserCode = Integer.parseInt(parts[2]);
                    String browserVer = parts[3];
                    int osCode = Integer.parseInt(parts[4]);
                    String osVer = parts[5];
                    long ip = Long.parseLong(parts[6]);
                    int locId = Integer.parseInt(parts[7]);
                    String domain = parts[8];
                    int sideId = Integer.parseInt(parts[9]);
                    int cId = Integer.parseInt(parts[10]);
                    String path = parts[11];
                    String referer = parts[12];
                    long guid = Long.parseLong(parts[13]);
                    String flashVersion = parts[14];
                    String jre = parts[15];
                    String sr = parts[16];
                    String sc = parts[17];
                    int geographic = Integer.parseInt(parts[18]);
                    String category = parts[23];
                    return new Log(timeCreate, cookieCreate, browserCode, browserVer, osCode, osVer, ip
                            , locId, domain, sideId, cId, path, referer, guid, flashVersion, jre, sr, sc, geographic, category);
                });
        Dataset<Row> logDF = sparkSession.createDataFrame(logRDD, Log.class );
        logDF.write().parquet("hdfs://localhost:9000/user/dat254/pageviewlog");
    }

    public static void ipMostGuid() {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark")
                .getOrCreate();
        Dataset<Row> log = sparkSession.read().parquet("hdfs://localhost:9000/user/dat254/pageviewlog");
        log.createOrReplaceTempView("log");
        Dataset<Row> result = sparkSession.sql("select ip, count(distinct guid) from log group by ip order by count(distinct guid) desc limit 1000");
    }

    public static void guidWithDifference() {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark")
                .getOrCreate();
        Dataset<Row> log = sparkSession.read().parquet("hdfs://localhost:9000/user/dat254/pageviewlog");
        log.createOrReplaceTempView("log");
        Dataset<Row> result = sparkSession.sql("select guid, timeCreate, cookieCreate, to_unix_timestamp(timeCreate) - to_unix_timestamp(cookieCreate) as differenceBySecond " +
                "from log where (to_unix_timestamp(timeCreate) - to_unix_timestamp(cookieCreate)) < 1800");
    }

    public static void mostUrlEachGuid() {
//        val df1 = spark.sql("select guid, concat(domain, path) as url, count(*) as num from log group by guid, domain, pa
//                th")
//        df1.withColumn("rank", rank().over(Window.partitionBy("guid").orderBy(col("num").desc))).filter("rank = 1")
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("spark")
                .getOrCreate();
        Dataset<Row> log = sparkSession.read().parquet("hdfs://localhost:9000/user/dat254/pageviewlog");
        log.createOrReplaceTempView("log");
        Dataset<Row> result = sparkSession.sql("select guid, concat(domain, path) as url, count(*) as num from logs group by guid, domain, path")
                .withColumn("rank", functions.rank().over(Window.partitionBy("guid").orderBy(new Column("num").desc()))).where("rank = 1");
    }
}