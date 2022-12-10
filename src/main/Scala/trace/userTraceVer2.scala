//package trace
//
///**
// * @Time：2021/5/17
// * @Author：JiQT
// * @File：userTraceVer2
// * @Software：IDEA
// **/
//
//import java.io.PrintWriter
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.apache.spark.sql.types.DoubleType
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.functions._
//
//import scala.collection.mutable.ArrayBuffer
//import scala.util.control.Breaks.{break, breakable}
//
///**
// * @Time：2021/5/16
// * @Author：JiQT
// * @File：userTrace
// * @Software：IDEA
// **/
//object userTraceVer2 {
//  private val EARTH_RADIUS: Double = 6378137.0 // 地球半径
//  def NowDate(): String = {
//    val now: Date = new Date()
//    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val date = dateFormat.format(now)
//    return date
//  }
//
//  def vectorDis(v1: Vector[Double], v2: Vector[Double]): Double = {
//    val lng1: Double = v1(0)
//    val lat1: Double = v1(1)
//    val lng2: Double = v2(0)
//    val lat2: Double = v2(1)
//    val radLat1: Double = lat1 * Math.PI / 180.0
//    val radLng1: Double = lng1 * Math.PI / 180.0
//    val radLat2: Double = lat2 * Math.PI / 180.0
//    val radLng2: Double = lng2 * Math.PI / 180.0
//    EARTH_RADIUS * Math.acos(Math.cos(radLat1) * Math.cos(radLat2) * Math.cos(radLng1 - radLng2) + Math.sin(radLat1) * Math.sin(radLat2))
//  }
//
//  def runDBSCAN(data: Array[Vector[Double]], ePs: Double, minPts: Int): (Array[Int], Array[Int]) = {
//    val overtime: String = NowDate
//    println("执行runDBSCAN开始时间："+overtime)
////    val treeNode: KDTree.TreeNode = KDTree.creatKdTree(data,2,2)
////    val node: KDTree.TreeNode = KDTree.nearestSearch(treeNode,data,200,3)
//    val types = (for (i <- data.indices) yield 2).toArray //用于区分核心点1，边界点0，和噪音点-1(即cluster中值为0的点)
//    val visited = (for (i <- data.indices) yield 0).toArray //用于判断该点是否处理过，0表示未处理过
//    var number = 1 //用于标记类
//    var xTempPoint = Vector(0.0, 0.0)
//    var yTempPoint = Vector(0.0, 0.0)
//    var distance = new Array[(Double, Int)](1)
//    var distanceTemp = new Array[(Double, Int)](1)
//    val neighPoints = new ArrayBuffer[Vector[Double]]()
//    var neighPointsTemp = new Array[Vector[Double]](1)
//    val cluster = new Array[Int](data.length) //用于标记每个数据点所属的类别
//    var index = 0
//    for (i <- data.indices) { //对每一个点进行处理
//      if (visited(i) == 0) { //表示该点未被处理
//        visited(i) == 1 //标记为处理过
//        xTempPoint = data(i) //取到该点
//        distance = data.map(x => (vectorDis(x, xTempPoint), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
//        neighPoints ++= distance.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点(密度相连点集合)
//
//        if (neighPoints.length > 1 && neighPoints.length < minPts) {
//          breakable {
//            for (j <- neighPoints.indices) { //此为非核心点，若其领域内有核心点，则该点为边界点
//              val index = data.indexOf(neighPoints(j))
//              if (types(index) == 1) {
//                types(i) = 0 //边界点
//                break
//              }
//            }
//          }
//        }
//        if (neighPoints.length >= minPts) { //核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
//          types(i) = 1
//          cluster(i) = number
//          while (neighPoints.nonEmpty) { //对该核心点领域内的点迭代寻找核心点，直到所有核心点领域半径内的点组成的集合不再扩大（每次聚类 ）
//            yTempPoint = neighPoints.head //取集合中第一个点
//            index = data.indexOf(yTempPoint)
//            if (visited(index) == 0) { //若该点未被处理，则标记已处理
//              visited(index) = 1
//              if (cluster(index) == 0) cluster(index) = number //划分到与核心点一样的簇中
//              distanceTemp = data.map(x => (vectorDis(x, yTempPoint), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
//              neighPointsTemp = distanceTemp.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点
//
//              if (neighPointsTemp.length >= minPts) {
//                types(index) = 1 //该点为核心点
//                for (x <- neighPointsTemp.indices) {
//                  //将其领域内未分类的对象划分到簇中,然后放入neighPoints
//                  if (cluster(data.indexOf(neighPointsTemp(x))) == 0) {
//                    cluster(data.indexOf(neighPointsTemp(x))) = number //只划分簇，没有访问到
//                    neighPoints += neighPointsTemp(x)
//                  }
//                }
//              }
//              if (neighPointsTemp.length > 1 && neighPointsTemp.length < minPts) {
//                breakable {
//                  for (t <- neighPointsTemp.indices) { //此为非核心点，若其领域内有核心点，则该点为边界点
//                    val index1 = data.indexOf(neighPointsTemp(t))
//                    if (types(index1) == 1) {
//                      types(index) = 0 //边界点
//                      break
//                    }
//                  }
//                }
//              }
//            }
//            neighPoints -= yTempPoint //将该点剔除
//          } //end-while
//          number += 1 //进行新的聚类
//        }
//      }
//    }
//    (cluster, types)
//  }
//
//  def main(args: Array[String]): Unit = {
//
//    val spark: SparkSession = SparkSession.builder()
//      .appName("trace_App")
//      .enableHiveSupport()
//      .getOrCreate()
//    val sc: SparkContext = spark.sparkContext  //建立连接
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
//
//    //    val sc = new SparkContext(conf)
//    val minPts = 3 //密度阈值
//    val ePs = 2000 //领域半径
//    val dim = 2 //数据集维度
//
//    //从hive读取数据
//    spark.sql("use user_trace")
//    val dataDF: DataFrame = spark.sql("select lon,lat from trace01")
//
//    val value = dataDF.rdd.map(row => {
//      val k: Double = row(0).toString.split("\n")(0).toDouble
//      val v=row(1).toString.split("\n")(0).toDouble
//      var vector = Vector[Double]()
//      vector ++= Vector(k,v)
//      vector
//    })
//    val points= value.glom().collect().flatten
//    val (cluster,types) = runDBSCAN(points,ePs,minPts)
//
//    //Map[(Int,Int),Array[((Int,Int),Vector[Double])]]
//    //Array[((簇Id，类型Id),点向量)]
//    val resultMat = cluster.zip(types).zip(points).groupBy(v => v._1)
//
//    val date: String = NowDate
//    println("runDBSCA执行完毕时间："+date)
//
//    val file_path="/home/jqt/Documents/res_full.txt"
//    //val raf=new RandomAccessFile(file_path,"rw")
//    val pw = new PrintWriter(file_path)
//    pw.flush()
//    //val writer_name = new BufferedWriter(new FileWriter(file))
//    resultMat.foreach(v=>{
//      val arr = v._2
//      arr.foreach(v=>{
//        pw.print(v._1._1)
//        pw.print(",")
//        pw.print(v._1._2)
//        pw.print(",")
//        pw.print(v._2(0))
//        pw.print(",")
//        pw.print(v._2(1))
//        pw.println()
//      })
//    })
//    pw.close()
//  }
//}
//
