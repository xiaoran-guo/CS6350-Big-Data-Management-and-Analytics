import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split, explode}

import org.apache.spark.graphx._
import org.apache.spark.sql.functions._

object RunExample {

  def main(args: Array[String]): Unit={
    val spark = SparkSession.builder().appName("Project").getOrCreate()
    //val spark = SparkSession.builder().master("local").appName("Project").getOrCreate()
    //import spark.implicits._

    var str = ""

    val inpath = args(0)
    //val inpath = "/Users/Mike/Documents/Summer2019/Project/yelp_dataset/user.json"
    val outpath = args(1)
    //val outpath = "/Users/Mike/Documents/Summer2019/Project/graphxOutput"
    val factor = args(2).toDouble

    val user = spark.read.option("multiLine",false).option("mode","PERMISSIVE")
      .json(inpath)
      .sample(false,factor)
        .persist()


    val userLookUp = user.select("user_id", "name", "fans").distinct()
      .withColumn("uniqueID", monotonically_increasing_id())
        .persist()


    val friend = user.select("user_id","friends").filter("friends != 'None'")
      .withColumn("friend_id",explode(split(user.col("friends"),", ")))
      .drop("friends")


    val friend1 = friend.join(userLookUp.select("user_id","uniqueID"), "user_id")
      .withColumnRenamed("uniqueID", "user_uniqueID")
      .drop("user_id")
        .persist()


    val friend2 = friend1.join(userLookUp.select("user_id","uniqueID","fans"),
      friend1.col("friend_id")===userLookUp.col("user_id"))
      .withColumnRenamed("uniqueID", "friend_uniqueID")
      .drop("friend_id","user_id")


    val user_edge = friend2.select("user_uniqueID","friend_uniqueID", "fans").rdd
      .map(row => Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue, row(2).asInstanceOf[Number]))

    val user_vertex = userLookUp.select("uniqueID","name","fans").distinct().rdd
      .map(row => (row(0).asInstanceOf[Number].longValue, (row(1).asInstanceOf[String], row(2).asInstanceOf[Long])))

    userLookUp.unpersist()
    user.unpersist()
    friend1.unpersist()

    val userGraph = Graph(user_vertex, user_edge).cache()

    // Top 10 users with most fans
    str=str+"\n"+"Top 10 users with most fans"
    //user_vertex.sortBy(_._2._2,false).take(10).foreach(println)
    userGraph.vertices.sortBy(_._2._2, false).take(10)
      .foreach(x => str=str+"\n\t"+"User "+x._2._1+" has "+x._2._2+" fans.")
    str=str+"\n"

    str=str+"\n"+"Top 10 users with most friends"
    userGraph.outDegrees.join(user_vertex).sortBy(_._2._1, false).take(10)
      .foreach(x => str=str+"\n\t"+"User "+x._2._2._1+" has "+x._2._1+" friends.")
    str=str+"\n"

    str=str+"\n"+"Top 10 users that can gain the most fans from their friends (PageRank)"
    val ranks = userGraph.pageRank(0.1).vertices.join(user_vertex)
    ranks.sortBy(_._2._1,false).take(10)
      .foreach(x => str=str+"\n\t"+"User "+x._2._2._1+" has rank "+x._2._1+";")
    str=str+"\n"


    str=str+"\n"+"Top 10 users with the largest triangle count"
    val triCounts = userGraph.triangleCount().vertices.join(user_vertex)
    triCounts.sortBy(_._2._1, false).take(10)
      .foreach(x => str=str+"\n\t"+"User "+x._2._2._1+" has triangle count "+x._2._1+";")
    str=str+"\n"

/*
    val scc = userGraph.stronglyConnectedComponents(5)
    scc.groupBy("component").count().show()
     */

    print(str)
    spark.sparkContext.parallelize(Seq(str))
      .saveAsTextFile(outpath)
  }
}
