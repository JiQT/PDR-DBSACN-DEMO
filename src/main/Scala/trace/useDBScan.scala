package trace

/**
 * @Time：2021/5/29
 * @Author：JiQT
 * @File：useDBScan
 * @Software：IDEA
 **/
import java.text.SimpleDateFormat
import java.util.Date


import dbscan.DBSCAN
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object useDBScan {
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }
  def main(args: Array[String]): Unit = {
    val date: String = NowDate
    println("开始时间："+date)
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkTrace").set("spark.executor.memory", "20G").set("spark.driver.memory", "10G")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext //建立连接
    val minPts =10//密度阈值
    val eps = 2200//领域半径
    //val dim = 2 //数据集维度
    val maxPointsPerPartition=100

    val data = sc.textFile("C:\\Users\\JiQT\\OneDrive\\dataset\\clusters-set\\unbalance2.txt")
//    val parsedData = data.map(s => Vectors.dense(s.split("\t").take(2).map(_.toDouble))).cache()

//    val traceData=sc.textFile("C:\\Users\\JiQT\\Desktop\\新建文件夹\\data\\2011\\select.txt")
  val parsedData = data.map(row => {
    val k: Double = row.split(" ", -1)(0).toDouble
    val v = row.split(" ", -1)(1).toDouble
    var vector = Vectors.dense(k, v)
    vector
  }).cache()
    //parsedTraceData.foreach(println(_))
    //log.info(s"EPS: $eps minPoints: $minPts")

    val model = DBSCAN.train(
      parsedData,
      eps = eps,
      minPoints = minPts,
      maxPointsPerPartition = maxPointsPerPartition)
    val date1: String = NowDate
    println("结束时间："+date1)
    //结果保存到本地
    val valueRDD = model.labeledPoints.map(p => (p.x, p.y, p.cluster))
    val path = "C:\\Users\\JiQT\\OneDrive\\dataset\\new_res\\rdd-unbalance2"
    val res: RDD[String] = valueRDD.map(x => x._1 + "," + x._2 + "," + x._3)
    res.repartition(1).saveAsTextFile(path)
  }
}
