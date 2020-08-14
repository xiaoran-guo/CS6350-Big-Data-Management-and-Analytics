/*

object AirlinesSentiment {

}
*/
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover, StringIndexer, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
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
import org.apache.spark.sql.functions._
import java.util.Scanner

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.corr
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spire.std.string

object CollaborativeFiltering {

  def main(args: Array[String]): Unit= {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("RunExample")
      .getOrCreate;

    val business=spark.read.option("multiLine", false).option("mode", "PERMISSIVE").json("business.json")
    business.printSchema()
    business.select("address","attributes","categories").show


    val scanner = new java.util.Scanner(System.in)

    print("Input your User ID ")
    val input1 = scanner.nextLine()

    print("Input your User ID ")
    val input2 = scanner.nextLine()

    print("Input your User ID ")
    val input3 = scanner.nextLine()


    val review=spark.read.option("multiLine", false).option("mode", "PERMISSIVE").json("review.json")
    review.drop("date")
    review.drop("cool")
    review.drop("funny")
//    review.drop("text")
    review.drop("useful")

    review.printSchema()
    review.select("funny","business_id","user_id","stars").show
    //val review = spark.read.option("header","true"). option("inferSchema","true"). csv("review_subset_1.csv").toDF()
    val review_subset = review.sample(0.01)
    //val Array(review_subset, big) = review.randomSplit(Array(0.001, 0.999))
    //review_subset.repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("review_subset1")


    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val remover = new StopWordsRemover().setInputCol(tokenizer.getOutputCol).setOutputCol("filtered")
    val indexer_bussiness = new StringIndexer()
      .setInputCol("business_id")
      .setOutputCol("bussiness_id_index")
    val indexer_user = new StringIndexer()
      .setInputCol("user_id")
      .setOutputCol("user_id_index")
    val pipeline = new Pipeline()
      .setStages(Array( indexer_bussiness,indexer_user,tokenizer,remover))

    val new_review = pipeline.fit(review_subset).transform(review_subset)



//    val indexed_temp = indexer_bussiness.fit(review_subset).transform(review_subset)
//    val indexed=indexer_user.fit(indexed_temp).transform(indexed_temp)
    val Array(training, test) = new_review.randomSplit(Array(0.8, 0.2))





    val als = new ALS()
      .setMaxIter(8)
      .setRegParam(0.01)
      .setImplicitPrefs(true)
      .setUserCol("user_id_index")
      .setItemCol("bussiness_id_index")
      .setRatingCol("stars")
//
    val paramGrid = new ParamGridBuilder()
      .addGrid(als.maxIter, Array(10,20))
      .build()


    val cv = new CrossValidator()
      //.setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)


    val model=als.fit(training)
    model.setColdStartStrategy("drop")

    //    val cvModel = cv.fit(training)
//    val recommandation = cvModel.transform(test)

    val predictions=model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("stars")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Error is  = $rmse")

    val userRecs = model.recommendForAllUsers(10)
    //var temp1=userRecs.select("recommendations")


//    val businessRecs = model.recommendForAllItems(10)
//
//    val users = indexed.select(als.getUserCol).distinct().limit(3)
    val users =new_review.where(new_review.col("user_id")===input1 || new_review.col("user_id")===input2 ||new_review.col("user_id")===input3).select(als.getUserCol)

    val userSubsetRecs = model.recommendForUserSubset(users, 7)

//    val businessRec = new_review.select(als.getItemCol).distinct().limit(3)
//    val uID="0vjJ41d_87M_wfunhnBQfg"
    val businessRec =new_review.where(new_review.col("user_id")===input1 || new_review.col("user_id")===input2 ||new_review.col("user_id")===input3).select(als.getItemCol)
    //val businessRec = spark.sql("SELECT user_id_index FROM new_review WHERE user_id = IQjxYnrntLdH8XsiXy8KMA")
//
    val businessSubSetRecs = model.recommendForItemSubset(businessRec, 7)


//    userRecs.repartition(1).rdd.saveAsTextFile("userRecommand")
//    businessRecs.repartition(1).rdd.saveAsTextFile("businessRecommand")
//    userSubsetRecs.repartition(1).rdd.saveAsTextFile("userSubsetRecs")
    userSubsetRecs.repartition(1).rdd.saveAsTextFile("users_Recommendations")
    businessSubSetRecs.repartition(1).rdd.saveAsTextFile("business_Recommendations")

    //println(userRecs.getClass.toString())
    //println(userSubsetRecs.getClass.toString())



//    val userRDD=userRecs.toJSON.rdd
//    userRDD
//      .map(_.trim.replaceAll("[\\[\\]]",""))
      //.map(_._1).foreach(println)
    userRecs.show()
//    businessRecs.show()
//    userSubsetRecs.show()
    userSubsetRecs.show()
    businessSubSetRecs.show()

    spark.stop()


    val user=spark.read.option("multiLine", false).option("mode", "PERMISSIVE").json("user.json")
    user.printSchema()
    user.select("compliment_cool","compliment_note","elite","fans","friends","useful","yelping_since").show


    val tip=spark.read.option("multiLine", false).option("mode", "PERMISSIVE").json("tip.json")
    tip.printSchema()

    val photo=spark.read.option("multiLine", false).option("mode", "PERMISSIVE").json("photo.json")
    photo.printSchema()

    val check_in=spark.read.option("multiLine", false).option("mode", "PERMISSIVE").json("checkin.json")
    check_in.printSchema()



  }
}
