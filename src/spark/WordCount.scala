package spark

import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by iwwenbo on 2016/4/29.
  */
object WordCount {
   def main(args: Array[String]) {
     //这里spark是单机模式，所以master为local
     val conf = new SparkConf().setMaster("local").setAppName("WordCount")
     val sc = new SparkContext(conf)
     val line = sc.textFile("hdfs://localhost:9000/user/input/wordcount.txt")
     line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
     sc.stop()
   }
 }
