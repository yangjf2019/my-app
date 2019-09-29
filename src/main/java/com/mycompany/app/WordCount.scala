package com.mycompany.app
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Created by Jeff Yang on 2019-09-29 11:48.
  * Update date:
  * Project: my-app
  * Package: com.mycompany.app
  * Describe :   
  * Dependency :  
  * Frequency: Calculate once a day.
  * Result of Test: test ok,test error
  * Command:
  *
  * Email:  highfei2011@126.com 
  * Status：Using online
  *
  * Please note:
  *    Must be checked once every time you submit a configuration file is correct!
  *    Data is priceless! Accidentally deleted the consequences!
  *
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val sc=new SparkContext(new SparkConf().setAppName("WordCount").setMaster("local[2]"))
    val arrays=Array(
      "1,2,3,4,d5,6,7,8,9,10",
      "11,2,3,4,5,6,7,38,9,10",
      "1,2,3,4,5,62,d7,8,9,10",
      "11,2,3,4,35,6,7,8,9,10",
      "1d,2,3,4,z5,62,7,8,9,10",
      "1,22,23,33,332,44,5,6,66,6,6,32"
    )
    val wordRDD=sc.parallelize(arrays)
    val flatMapRDD=wordRDD.flatMap(_.split(","))
    val mapRDD=flatMapRDD.map((_,1L))
    val reduceRDD=mapRDD.reduceByKey((x,y)=>x+y)
    reduceRDD.foreach(println)
    sc.stop()
  }

}
