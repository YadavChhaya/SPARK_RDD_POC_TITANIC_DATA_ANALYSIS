package org.spark.titanic.mypack
/**
 * APACHE SPARK POC for TITANIC DATA ANALYSIS
 * @author Chhaya Yadav
 * Compiled on 21th July 2018
 * version 1.0
 */
//Project Description for Titanic Data Analysis 
//Problem Statement 1:
//In this problem statement, we will find the average age of males and females who died in the Titanic tragedy.
//Problem Statement 2:
//In this problem statement, we will find the number of people who died or survived in each class, along with their gender and age.

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

object titanicobject {
  
  def main(args: Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("TitanicDataAnalysis").setMaster("local")
    
    val sc = new SparkContext(conf)
//Problem Statement1
    
    val rdd11 = sc.textFile("hdfs://localhost:9000/user/hadoop/input/TitanicData.txt")
    val rdd12 = rdd11.filter { x => {if(x.toString().split(",").length >= 6) true else false} }
    val rdd13 = rdd12.map(line=>{line.toString().split(",")})
    val rdd14 = rdd13.filter{x=>if((x(1)=="1")&&(x(5).matches(("\\d+"))))true else false}
    val rdd15 = rdd14.map(x => {(x(4),x(5).toInt)})
    val rdd16 = rdd15.mapValues((_, 1))
    val rdd17 = rdd16.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val rdd18 = rdd17.mapValues{ case (sum, count) => (1.0 * sum)/count}
    rdd18.collectAsMap()
    rdd18.saveAsTextFile("hdfs://localhost:9000/user/hadoop/output/extract1")
    
//Problem Statement 2
    val rdd21 = sc.textFile("hdfs://localhost:9000/user/hadoop/input/TitanicData.txt")
    val rdd22 = rdd21.filter { x => {if(x.toString().split(",").length >= 6) true else false} }
    val rdd23 = rdd22.map(line=>{line.toString().split(",")})
    val rdd24 = rdd23.map(x => {(x(1)+" "+x(2)+" "+x(4)+" "+x(5),1)})
    val rdd25 = rdd24.reduceByKey(_+_)
    rdd25.collectAsMap()
    rdd25.saveAsTextFile("hdfs://localhost:9000/user/hadoop/output/extract1")
    
    

    
  }
}