package main.org.bigscala.connector

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Created by lipingzhang on 09/24/2015.
 */
object GenerateData {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("sql")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val col_a = Array(1.0, 2, 3, 4)
    val col_b = Array(2, 3, 4, 5)
    val df1 = sqlContext.createDataFrame(col_a.zip(col_b)).toDF("a", "b")

    val col_c = Array(1.0, 2, 3, 4)
    val col_d = Array(2, 3, 4, 5)
    val df2 = sqlContext.createDataFrame(col_c.zip(col_d)).toDF("c", "d")
    val col_p1 = df2.col("c") + 1
    val df3 = df2.withColumn("a", col_p1)
    df3.show(false)

    val df4 = df3.withColumnRenamed("a", "aa")
    df4.show(false)

    val result = df1.join(df3, Seq("a"))

  }

}

