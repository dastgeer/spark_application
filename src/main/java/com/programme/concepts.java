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

}
