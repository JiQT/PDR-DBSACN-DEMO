package trace

import dbscan.DBSCAN
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Date

object useDBScanV2 {
  def main(args: Array[String]): Unit = {
    var start_time = new Date().getTime

    val spark: SparkSession = SparkSession.builder()
      .appName("RDD_DBscan_App")
      .enableHiveSupport()
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext //建立连接

    val minPts = Integer.parseInt(args(0)) //密度阈值

    val Eps = Integer.parseInt(args(1)) //领域半径
    //数据集维度
    val maxPointsPerPartition= Integer.parseInt(args(2))

    spark.sql("use datasets")
    val dataDF: DataFrame = spark.sql("select x,y from new20")

    val parsedData = dataDF.rdd.map(row => {
      val k: Double = row(0).toString.split("\n")(0).toDouble
      val v = row(1).toString.split("\n")(0).toDouble
      var vector = Vectors.dense(k, v)
      vector
    }).cache()
    //    var i=0
    //    val duplicated = for {
    //      point <- parsedData.map(DBSCANPoint)
    //    } yield point
    //    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkTrace").set("spark.executor.memory","6G").set("spark.driver.memory","3G")
    //    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //    val sc: SparkContext = spark.sparkContext  //建立连接
    //    val minPts = 5//密度阈值3
    //    val minimumRectangleSize = 8    //拆分区域最小边长
    //
    //    val Partition=31  //聚类数
    //    // 本地读取数据
    //    val data: RDD[String] = sc.textFile("C:\\Users\\z'j\\Desktop\\dataset\\clusters-set\\D31.txt")
    //    val parsedData= data.map(row=> {
    //      val k: Double = row.split("\\s")(0).toDouble
    //      val v = row.split("\\s")(1).toDouble
    //      var vector = Vectors.dense(k,v)
    //      vector
    //    }).cache()
    //val labeledPoints = new LocalDBSCAN(ePs, minPts).fit()

    //调用DBSCANGeo传参建模
    val model = DBSCAN.train(
      parsedData,
      eps = Eps,
      minPoints = minPts,
      maxPointsPerPartition = maxPointsPerPartition)
    //结束时间
    var end_time = new Date().getTime
    println("总耗时：" + ((end_time - start_time) / 1000) + "s")
    //model.labeledPoints.map(p =>  s"${p.x},${p.y},${p.cluster}").coalesce(1,true).saveAsTextFile("file:///home/jqt/Documents/D31Res")


    //    val valueRDD= model.labeledPoints.map(p => (p.x,p.y,p.cluster))
    //    val path="C:\\Users\\z'j\\Desktop\\dataset\\D31"
    //    val res = valueRDD.map(x => x._1 + "," + x._2+","+x._3)
    //    res.repartition(1).saveAsTextFile(path)

    import spark.implicits._
    val valueRDD = model.labeledPoints.map(p => (p.x, p.y, p.cluster))
    //结果保存到Hive
    val resDF: DataFrame = valueRDD.toDF("px", "py", "cluster")
    resDF.createOrReplaceTempView("temp")
    spark.sql("create table resNew20 as select * from temp")
    spark.catalog.dropTempView("temp")
  }

}
