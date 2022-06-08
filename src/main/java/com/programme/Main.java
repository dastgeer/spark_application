package com.programme;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Main {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\winutils");
        List<Integer> inputData = new ArrayList();
        inputData.add(10);
        inputData.add(20);
        inputData.add(30);
        inputData.add(40);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc= new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(inputData);
        Integer reduceData = rdd.reduce((val1, val2) -> val1 + val2);
        JavaRDD<Double> sqrtRDD = rdd.map(Math::sqrt);
        sqrtRDD.foreach(value->System.out.println(value));
        System.out.println(reduceData);
        sc.close();
    }
}
