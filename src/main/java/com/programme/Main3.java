package com.programme;

import com.programme.util.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class Main3 {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\winutils");
        List<Tuple2<Integer,Integer>> visitData = new ArrayList();
        visitData.add(new Tuple2(4,8));
        visitData.add(new Tuple2(6,16));
        visitData.add(new Tuple2(10,24));


        List<Tuple2<Integer,String>> userRawData = new ArrayList();
        userRawData.add(new Tuple2(1,"adam"));
        userRawData.add(new Tuple2(2,"adim"));
        userRawData.add(new Tuple2(3,"kishan"));
        userRawData.add(new Tuple2(4,"hello"));
        userRawData.add(new Tuple2(5,"komal"));
        userRawData.add(new Tuple2(6,"tina"));
        userRawData.add(new Tuple2(4,"prerna"));
//basically we configured hadoop here to run in local mode.
     //   as * in saying that run this prohramme with multithread as it is available under the system capabilty and cores availabilty.
       // but when we deploy this in hadoop cluster which might be aws cluster server , if this will local then it will only leaverage the master(which is driver) node and rest of
        //its child node or worker node( called as executive node) will sitting idle. remove setmaster() while deployinh in cluster env
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc= new JavaSparkContext(conf);
//when we are creating  RDD from in-memory() which is driver node at that time we have to use only sc.parallelize() on collection
        //but when we want to create pair RDD from in-memory from driver node then we have to use below cmd
        JavaPairRDD<Integer, Integer> visitDataRDD = sc.parallelizePairs(visitData);
        JavaPairRDD<Integer, String> userRawDataRDD = sc.parallelizePairs(userRawData);
//        //it is inner join  as left table and right table and join on leftid =right id
//        JavaPairRDD<Integer, Tuple2<Integer, String>> innerjoinedRDD = visitDataRDD.join(userRawDataRDD);
//        innerjoinedRDD.foreach(value->System.out.println(value));
//
//        //this is give the allrecord from left table and matching records from right table,if left doesnt match with right then right portion will be null
//        // for null part handled with optional that is if value willnot present instrad of null pointer return default value
//        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>>  leftOuterJoinedRDD= visitDataRDD.leftOuterJoin(userRawDataRDD);
//        leftOuterJoinedRDD.foreach(value->value._2._2.orElse("blank").toUpperCase());
//
//
//        //this is give the matched record from left table and all records from right table,if right doesnt match with left then left portion will be null
//        // for null part handled with optional that is if value willnot present instrad of null pointer return default value
//        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinRDD = visitDataRDD.rightOuterJoin(userRawDataRDD);
//        rightOuterJoinRDD.foreach(value-> System.out.println("user value "+value._2._1.orElse(0)));

        //this is give the matched record from left table and right table  along with unmatched all records from right table & left table,if right & left then those portion will be null/empty
        // for null part handled with optional that is if value willnot present instrad of null pointer return default value
//        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullouterJoinJavaPairRDD = visitDataRDD.fullOuterJoin(userRawDataRDD);
//        fullouterJoinJavaPairRDD.foreach(value-> System.out.println(value));

        //this is give prodcut of the n record from left table and  m record right table ,  unmatched all records from right table & left table,if unmatched from right & left then those portion will be null/empty
        // for null part handled with optional that is if value willnot present instrad of null pointer return default value
        //it is cross product of n rows from left with m rows with right called cartesion
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoinedRDD = visitDataRDD.cartesian(userRawDataRDD);
        cartesianJoinedRDD.foreach(value-> System.out.println(value));

        sc.close();
    }
}
