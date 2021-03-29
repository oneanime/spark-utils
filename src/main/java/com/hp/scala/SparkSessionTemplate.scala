package com.hp.scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTemplate {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "bigdata")
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    doSomething(spark, args)
    spark.close()
  }

  def doSomething(spark: SparkSession, args: Array[String])
}
