import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix

object SVD {

  def main(args: Array[String]): Unit= {

    val spark = SparkSession.builder().appName("Project").getOrCreate()
    import spark.implicits._

    var str = ""

    val inpath = args(0)

    val outpath = args(1)

    val factor = args(2).toDouble


    val review = spark.read.option("multiLine",false).option("mode","PERMISSIVE")
      .json(inpath)
      .drop("date","text","useful","funny","cool")
      .sample(false,factor)
      .cache()

    val userLookUp2 = review.select("user_id").distinct()
      .rdd.zipWithIndex.map{case(row, id) => (row(0).toString,id.toLong)}
      .toDF("user_id","user_uniqueID")

    val businessLookUp2 = review.select("business_id").distinct()
      .rdd.zipWithIndex.map{case(row, id) => (row(0).toString,id.toInt)}
      .toDF("business_id","business_uniqueID")

    val user_count = userLookUp2.count()
    val bus_count = businessLookUp2.count()

    val review1 = review.join(userLookUp2, "user_id")
      .drop("user_id")

    val review2 = review1.join(businessLookUp2, "business_id")
      .drop("business_id")
      .cache()

    val review21 = review2.groupBy("user_uniqueID","business_uniqueID").avg("stars")
      .sort("user_uniqueID","business_uniqueID")

    val Array(training, test) = review21.randomSplit(Array(0.9, 0.1))

    // 1

    val review22 = review21.rdd
      .map(row => (row(0).asInstanceOf[Long],(row(1).asInstanceOf[Int],row(2).asInstanceOf[Double])))
      .groupByKey()

    val review3y = review22
      .map(row => {
        var temp1 = new Array[Double](bus_count.toInt)
        row._2.foreach(e => temp1(e._1)=e._2)
        IndexedRow(row._1,Vectors.dense(temp1))
      })

    // 2

    val mat3: IndexedRowMatrix = new IndexedRowMatrix(review3y,user_count,bus_count.toInt)

    val svd: SingularValueDecomposition[IndexedRowMatrix, Matrix] = mat3.computeSVD(25, computeU = true)
    val U: IndexedRowMatrix = svd.U  // The U factor is a RowMatrix.
    val s: Vector = svd.s     // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V     // The V factor is a local dense matrix.

    // 3

    val S = Matrices.diag(s)

    val pred_mat = U.multiply(S).multiply(V.transpose)

    val sorted = pred_mat.rows.sortBy(_.index)

    // 4

    val label1 = test.rdd
      .map(row => (row(0).asInstanceOf[Long],(row(1).asInstanceOf[Int],row(2).asInstanceOf[Double])))

    val pred1 = sorted.map(x => {
      (x.index, x.vector)
    })

    val valid1 = label1.join(pred1).mapValues{case ((idbus, ratebus), pred_v) =>
      var diff = ratebus - pred_v.apply(idbus)
      diff*diff
    }

    val valid2 = valid1.reduce{case ((x1,y1),(x2,y2)) =>
      (1L, y1+y2)
    }

    str=str+"Number of users: "+user_count+"\n"
    str=str+"Number of businesses: "+bus_count+"\n"

    str = str+"RMSE result: \n"
    str = str+Math.sqrt(valid2._2/label1.count())+"\n"

    spark.sparkContext.parallelize(Seq(str))
      .saveAsTextFile(outpath)

  }

}
