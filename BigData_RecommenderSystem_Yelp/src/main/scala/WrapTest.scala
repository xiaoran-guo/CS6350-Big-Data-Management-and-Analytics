import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover, StringIndexer, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import java.io._

import scala.collection.mutable
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WrapTest {

  def main(args: Array[String]): Unit = {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Airlines Sentiment")
    val sc = new SparkContext(conf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

//    val input = spark.read.option("inferSchema", "true").option("header", "false")
//      .csv("user_rdd.txt").toDF("aaa","bbb")


    val review=spark.read.option("multiLine", false).option("mode", "PERMISSIVE").json("review.json")
    val review_subset = review.sample(0.001)
    review_subset.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("review_subset1")


    //    var input=sc.textFile("user_rdd.txt")
//    //println(input.getClass.toString())
//    var temp1=input
//      .map(_.trim.replaceAll("[\\[\\]]",""))
//      .map(_.trim.replaceAll("(\\(\\))",""))
//      .map(_.trim.replaceAll("WrappedArray",""))
//      .take(1).filter(x=>x>10).foreach(println)

    //val temp2 = sc.parallelize(temp1)

  }
}