package com.programme;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main4 {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\winutils");
        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // Use true to use hardcoded if false read data from file.
        boolean testMode = true;
        JavaPairRDD<Integer, Integer> viewDataRDD = setUpViewDataRdd(javaSparkContext, testMode);
        JavaPairRDD<Integer, Integer> chapterDataRDD = setUpChapterDataRDD(javaSparkContext, testMode);
        JavaPairRDD<Integer, String> titleDataRDD = setUpTitlesDataRdd(javaSparkContext, testMode);

        JavaPairRDD<Integer, Integer> reducedCourseIDChapterCountPairRDD = chapterDataRDD.mapToPair(eachRows -> new Tuple2<>(eachRows._2, 1))
                .reduceByKey((value1, value2) -> value1 + value2);
      //  reducedCourseIDChapterCountPairRDD.foreach(data->System.out.println(data));
        //viewDataRDD.foreach(value-> System.out.println(value));

        /*step -1 this is to remove data from pair , if any one side matches fine but whole row or tuple datas should not same*/
        JavaPairRDD<Integer, Integer> distinctViewRDD = viewDataRDD.distinct();
        //distinctViewRDD.foreach(value-> System.out.println(value));

        /* step2 - flip to userid with chapter id to further process easy. this will do inner join with same chapterid from both table/rdd*/

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> inerJoinOfViewwithChapter = distinctViewRDD.mapToPair(eachRow -> new Tuple2<Integer, Integer>(eachRow._2, eachRow._1))
                .join(chapterDataRDD);
       // step 3: inerJoinOfViewwithChapter.foreach(value-> System.out.println(value));

        JavaPairRDD<Tuple2<Integer, Integer>, Long> userIdcourseIdRDD = inerJoinOfViewwithChapter.mapToPair(eachRowData -> {
            return new Tuple2<Tuple2<Integer, Integer>, Long>(new Tuple2(eachRowData._2._1, eachRowData._2._2), 1l);
        });
        //step 4: count how man view per user per course
        JavaPairRDD<Tuple2<Integer, Integer>, Long> userIdCourseIDViewRDD = userIdcourseIdRDD.reduceByKey((value1, value2) -> {
            return value1 + value2;
        });

        //step 5 remove user id from tuple pair and keep on course id
        JavaPairRDD<Integer, Long> courseidViewCountReducedRDD = userIdCourseIDViewRDD.mapToPair(row -> new Tuple2<Integer, Long>(row._1._2, row._2));

        //step 6:join the courseid view count RDD with reducedCourseID_ChapterCountPairRDD , it will do the inner join on course id and join the view on left with chapter number
        //it means result will have course 1 have thet count of view with this chapter number.
        JavaPairRDD<Integer, Tuple2<Long, Integer>> joinedCourseIdWithviewCountChapterNum = courseidViewCountReducedRDD.join(reducedCourseIDChapterCountPairRDD);
      //  joinedCourseIdWithviewCountChapterNum.foreach(data-> System.out.println(data));


        //step:7 convert that tuple data to single value as va;ue will in % for correspoding key
        JavaPairRDD<Integer, Double> coursIdPercentageUsageRDD = joinedCourseIdWithviewCountChapterNum.mapValues(value -> (double) value._1 / value._2);
        //coursIdPercentageUsageRDD.foreach(data-> System.out.println(data));

        //step 8: now map % value with logic of score
        JavaPairRDD<Integer, Long> courseIdScoreIdRDD = coursIdPercentageUsageRDD.mapValues(value -> {
            if (value > 0.9) return 10L;
            if (value > 0.5) return 4L;
            if (value > 0.25) return 2L;
            return 0L;
        });
        //step 9: now reduce with unique course and count it.
        JavaPairRDD<Integer, Long>  reducedWithCourseIdRankedCount = courseIdScoreIdRDD.reduceByKey((value1, value2) -> value1 + value2);

        //step 10 joining table with titleDataRDD on course id data
        JavaPairRDD<Integer, Tuple2<Long, String>> joinCourseCountTitleWithCourseidRDD = reducedWithCourseIdRankedCount.join(titleDataRDD);

        //step 11: remove course id & now sort in descending order based on key
        JavaPairRDD<Long, String>  rankCountOfCourseWithCourseTitleRDD= joinCourseCountTitleWithCourseidRDD.mapToPair(row -> new Tuple2<>(row._2._1, row._2._2)).sortByKey(false);
        rankCountOfCourseWithCourseTitleRDD.foreach(data-> System.out.println(data));

    }

    private static JavaPairRDD<Integer,String> setUpTitlesDataRdd(JavaSparkContext javaSparkContext, boolean testMode) {
        if(testMode)
        {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return javaSparkContext.parallelizePairs(rawTitles);
        }
        return javaSparkContext.textFile("src/main/resources/viewing figures/titles.csv").mapToPair(commaSeparatedLine->{
            String[] cols = commaSeparatedLine.split(",");
            return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
        });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRDD(JavaSparkContext javaSparkContext, boolean testMode) {
        if(testMode){
            // (chapterId, (courseId, courseTitle))
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96,  1));
            rawChapterData.add(new Tuple2<>(97,  1));
            rawChapterData.add(new Tuple2<>(98,  1));
            rawChapterData.add(new Tuple2<>(99,  2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return javaSparkContext.parallelizePairs(rawChapterData);
        }
        return javaSparkContext.textFile("src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSperateLine->{
                    String[] split = commaSperateLine.split(",");
                    return new Tuple2<>(new Integer(split[0]),new Integer(split[1]));
                });

    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext javaSparkContext, boolean testmode) {
        if (testmode)
        {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return javaSparkContext.parallelizePairs(rawViewData);
        }
        return javaSparkContext.textFile("src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedLine->{
                    String[] split = commaSeparatedLine.split(",");
                    return new Tuple2<Integer,Integer>(new Integer(split[0]), new Integer(split[1]));
                }) ;
    }
}
