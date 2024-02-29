package com.programme;

public class concepts {
    //1. spark config
    //2.spark context
    //3.partitioned
    //4. RDD,PairRDD
    //5.map,maptoPair()
    //6.tuple2,3,4....
    //7.filter,reduce,count,flatmap,take(no of element to get for next step) of PairRDD
    //8.groupbyKey,reduceByKey,sortByKey
    //9.textfile() to read file from local /distributed system
    //10javaRdd.getNumPartion() return no odf partion on current macine,nbased on that it will run and assign thread to perform task.
//by default it is 2.so interanlly every RDD perform task by distributing the datset itno 2 Partition and return to abck,because driver class which is main passign function
    //to it to perform operation on RDD with all partitions.
    //11. coalesce()  it will get all dataset from all partition in to one RDD and perform operations.
    //12. collect() it will collect the data from all partition of RDD and put into single RDD and perform it can be crash that node server that make
    //out of memory exception. because it will load all dataset into RAM only.

    //if we want to see how may job is executing , and all details on UI , we can see using spark UI at adress until
    // our job is running.http://localhost:4040/jobs/.
    // there can be see all details of and their dag(drect acyclic graph) for each execution steps whichi
    // is transformation (always lazy load will excute only when terminal or action function get call,till before it
    // will for m the exution graph order in order to execute when it get called with action .)and the , some of
    // function which action function like take(10) or sortBykey() etc like termincal funtion is java those execution
    // pipeline will be act as placeholder . it wont create new object of JavaRDD at each step it will create only
    // one when it been called by any lazily method.

   /* narrow & wide transformation.*/
   // 1. driiver pass the lampda expression to of worker node and worked node with have the chunk of parttion from
    // the RDD . and worker node procsse the lamda or function against each of the partitioned chunk and get those
    // result after that will ave the data.
    //spark implememt the transformation without moving any data around and this way it is called narrow transformation.
    //wide transfromation is expensive transformation , when ever requirement for shuffiling of data or segegating
    // data from by using some groupbykey , in this suitation partitions are scattered at multiple worker node and
    // each of them have to do it. so those data will be in some object have to convert in to binary and transform to
    // another worker node where we can aggregate by key it willbe pass through network by doing serialization  it
    // will be costlier ahd heavy operations. such kind of things called wide trasformation.


}
