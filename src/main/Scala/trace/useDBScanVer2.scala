//package trace
//
//import java.text.SimpleDateFormat
//import java.util.Date
//import java.lang.Double
//
//import ch.hsr.geohash.GeoHash
//import dbscan.DBSCAN
//import org.apache.spark.SparkContext
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.DoubleType
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
///**
// * @Time：2021/5/30
// * @Author：JiQT
// * @File：useDBScanVer2
// * @Software：IDEA
// * @使用rtree结合DBSCAN
// **/
//object useDBScanVer2 {
//  def NowDate(): String = {
//    val now: Date = new Date()
//    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val date = dateFormat.format(now)
//    date
//  }
//
//  def main(args: Array[String]): Unit = {
//    val date: String = NowDate
//    println("开始时间："+date)
//    val spark: SparkSession = SparkSession.builder()
//      .appName("dbscan_App_archery")
//      .enableHiveSupport()
//      .getOrCreate()
//    val sc: SparkContext = spark.sparkContext  //建立连接
//    val minPts = Integer.parseInt(args(0)) //密度阈值
//    val eps=Double.parseDouble(args(1))  //领域半径
//    //val dim = 2 //数据集维度
//    val maxPointsPerPartition= Integer.parseInt(args(2))
//
////    spark.sql("use datasets")
////    val dataDF: DataFrame = spark.sql("select px,py from jain")
//    spark.sql("use user_trace")
//    val dataDF: DataFrame = spark.sql("select longitude,latitude from trace03")
//
//    val parsedData = dataDF.rdd.map(row => {
//      val k: Double = row(0).toString.split("\n")(0).toDouble
//      val v=row(1).toString.split("\n")(0).toDouble
//      var vector = Vectors.dense(k,v)
//      vector
//    }).cache()
//
//    val model = DBSCAN.train(
//      parsedData,
//      eps = eps,
//      minPoints = minPts,
//      maxPointsPerPartition = maxPointsPerPartition)
//    val date1: String = NowDate
//    println("结束时间："+date1)
//    //model.labeledPoints.map(p =>  s"${p.x},${p.y},${p.cluster}").coalesce(1,true).saveAsTextFile("file:///home/jqt/Documents/traceRes")
//    val valueRDD= model.labeledPoints.map(p => (p.x,p.y,p.cluster) )
//    import spark.implicits._
//    val resDF: DataFrame = valueRDD.toDF("longitude","latitude","cluster")
//    resDF.createOrReplaceTempView("temp")
//    spark.sql("create table resTrace as select * from temp")
//    spark.catalog.dropTempView("temp")
//    sc.stop()
//  }
//}
