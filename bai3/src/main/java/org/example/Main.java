package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

    public void getIp() {
//        spark.sql("select ip, count(distinct guid) from log group by ip having count(distinct guid) = (select max(num) from (select count(distinct guid) as num from log group by ip))").show(1000)
    }

    public void getGuid() {
//        spark.sql("select guid, timeCreate, cookieCreate, to_unix_timestamp(timeCreate) - to_unix_timestamp(cookieCreate) as differenceBySecond from log where (to_unix_timestamp(timeCreate)
//                - to_unix_timestamp(cookieCreate)) < 1800").show()
    }

    public void getUrl() {
//        spark.sql("select guid, concat(domain, path) as url from log as outerLog where to_date(timeCreate) = \"2018-08-10\" group by guid, domain, path having count(*) = (select max(num) from (select count(*) as num from log as innerLog where outerLog.guid = innerLog.guid and to_date(timeCreate) = \"2018-08-10\" group by guid, domain, path))").show(false)
    }
}