//package trace
//
///**
// * @Time：2021/5/16
// * @Author：JiQT
// * @File：userTrace
// * @Software：IDEA
// **/
//
//import java.io.PrintWriter
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.mutable.ArrayBuffer
//import scala.util.control.Breaks.{break, breakable}
//
//object userTrace {
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
//  def vectorDis1(v1: Vector[Double], v2: Vector[Double]):Double = {
//    var distance = 0.0
//    for(i <- 0 to v1.length - 1){
//      distance += (v1(i) - v2(i)) * (v1(i) - v2(i))
//    }
//    distance = math.sqrt(distance)
//    distance
//  }
//
//  def runDBSCAN(data: Array[Vector[Double]], ePs: Double, minPts: Int): (Array[Int], Array[Int]) = {
//    val overtime: String = NowDate
//    println("执行runDBSCAN开始时间："+overtime)
//
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
//        distance = data.map(x => (vectorDis1(x, xTempPoint), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
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
//              distanceTemp = data.map(x => (vectorDis1(x, yTempPoint), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
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
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkTrace")
//    //conf.set("spark.cleaner.referenceTracking","false")
//    //conf.set("spark.cleaner.referenceTracking.blocking","false")
//    //    val sc = new SparkContext(conf)
//    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//    val sc: SparkContext = spark.sparkContext  //建立连接
//    val minPts = 4 //密度阈值
//
//    val ePs = 1.39 //领域半径
//    val dim = 2 //数据集维度
//
//    //从本地读取数据
//    val RDD: RDD[String] = sc.textFile("C:\\Users\\JiQT\\Desktop\\专业材料\\论文准备材料\\dataset\\jain.txt")
//
//    val selectRDD= RDD.map(line=> {
//      val strings=line.toString.split(",").take(2).map(_.toDouble)
//      var vector = Vector[Double]()
//      for (i <- 0 to dim - 1) {
//        vector ++= Vector(strings(i))
//      }
//      vector
//    })
//
//    //RDD转为Array
//    val points: Array[Vector[Double]] = selectRDD.glom().collect().flatten
//    val (cluster,types) = runDBSCAN(points,ePs,minPts)
//
//    //Map[(Int,Int),Array[((Int,Int),Vector[Double])]]
//    val resultMat = cluster.zip(types).zip(points).groupBy(v => v._1) //Array[((Int,Int),Vector[Double])],即Array[((簇Id，类型Id),点向量)]
//
//    val date: String = NowDate
//    println("runDBSCA执行完毕时间："+date)
//    val file_path="C:\\Users\\JiQT\\Desktop\\专业材料\\论文准备材料\\resData\\res1.txt"
//    //结果写入文件
//    val pw = new PrintWriter(file_path)
//    pw.flush()
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
//
//    //数组转为RDD
//    //val rdd: RDD[Map[Int, Array[(Int, Vector[Double])]]] = sc.parallelize(Seq(result))
//    //RDD存储到HDFS路径和Linux本地路径
//    //res.repartition(1).saveAsTextFile("/home/jqt/Documents/res")
//    // res.repartition(1).saveAsTextFile("file:///home/jqt/Documents/res")
//
//  }
//
//}
//
