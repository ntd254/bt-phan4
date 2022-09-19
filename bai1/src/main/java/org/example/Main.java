package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;


public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("kMean").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        String path = "./src/main/resources/output_4.txt";
        JavaRDD<String> data =  jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] numbs = s.split("\\|");
            double[] values = new double[numbs.length];
            for (int i = 0; i < numbs.length; i++) {
                values[i] = Double.parseDouble(numbs[i]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();
        int numClusters = 3;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        jsc.close();
    }
}