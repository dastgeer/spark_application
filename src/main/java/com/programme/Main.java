package com.programme;

import com.programme.model.IntegerSqrtPair;
import com.programme.util.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple2$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

@Slf4j
public class Main {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\winutils");
//        List<Integer> inputData = new ArrayList();
//        inputData.add(10);
//        inputData.add(20);
//        inputData.add(30);
//        inputData.add(40);
//basically we configured hadoop here to run in local mode.
     //   as * in saying that run this prohramme with multithread as it is available under the system capabilty and cores availabilty.
       // but when we deploy this in hadoop cluster which might be aws cluster server , if this will local then it will only leaverage the master(which is driver) node and rest of
        //its child node or worker node( called as executive node) will sitting idle. remove setmaster() while deployinh in cluster env
        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc= new JavaSparkContext(conf);

        //the problem here now reading this text file from local but when we deploy this on cluster then it will not avialble on cluster
        //if it is running onlocal it willassume that it is path of file  as assumption by spark architectural design
        //either it will be hadoop file system file(hdfs) or s3 bucket file. then add here pass s3 bucket or hadfs file path as absolute.
        JavaRDD<String> inputStringJavaRDD = sc.textFile("src/main/resources/subtitles/input.txt");
        JavaRDD<String> map = inputStringJavaRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())//it will replace all chracter except a-z-Az and space
                                                .filter(sentence -> sentence.trim().length()>1)//it will remove space tabs and only allow sentence have length >1
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())// to convert sentence into words which return collection in to flatmap whihc will put in normal RDD
                  .filter(word-> word.trim().length()>0)
                .filter(words-> Util.isNotBoring(words));

        JavaPairRDD<String, Long> stringLongJavaPairRDD = map.mapToPair(word -> new Tuple2<>(word, 1l));

        JavaPairRDD<String, Long> stringLongJavaPairRDD1 = stringLongJavaPairRDD.reduceByKey((value1, value2) -> value1 + value2);
// now flit the pair RDD with long,string
        JavaPairRDD<Long, String> flippedValueTOKeyRDD = stringLongJavaPairRDD1.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        JavaPairRDD<Long, String> sortedKeyRDD1 = flippedValueTOKeyRDD.sortByKey(false);
        System.out.println("printing number of default partitionin my machine "+sortedKeyRDD1.getNumPartitions());// no of default internally partiton is 2
        List<Tuple2<Long,String>> takenDataList = sortedKeyRDD1.take(50);// if we want small amount of data by specify number those num of data it will fetch .
        takenDataList.forEach(System.out::println);
        //JavaRDD<Integer> originalRDD = sc.parallelize(inputData);
        //in this way can be achieve through java general way by setting ,but we can also achive through scal concept implemented here tuple
     //   JavaRDD<IntegerSqrtPair> mapppedRDD = originalRDD.map(value -> new IntegerSqrtPair(value));

        //tuple is concept like in python list using () these brackets and passing value into it as list, in similar way scala also
        //have () and as many element you enter that many type of element tuple will be created in scala but in our java spark
        //it has provided package as scala and with "new Tuple2(1,2.0)" there is 2 value in it will have type of <integer,double>
        //we can create any number of element tuple in object .like eg:"new Tuple22("",,,,,,....)" like we have to store 22 type in one object and return type will be same.

        //to achive mappedRDD using tuple
       // eg: Tuple2<Integer, Double> integerDoubleTuple2 = new Tuple2<>(1, 2.0);, we will use tuple acorss the spark
     //   JavaRDD<Tuple2<Integer,Double>> mappedToTupleRDD = originalRDD.map(value -> new Tuple2(value, Math.sqrt(value)));
        //mapppedRDD.mapToPair()// this is to map in 2 column as key value like map but this will not have key unique fascilty and add duplicate key as well

//        Integer reduceData = rdd.reduce((val1, val2) -> val1 + val2);
//        JavaRDD<Double> sqrtRDD = rdd.map(Math::sqrt);
//        sqrtRDD.foreach(value->System.out.println(value));
//        System.out.println(reduceData);
        Scanner sc1 = new Scanner(System.in);
        sc1.nextLine();
        sc.close();
    }
}
