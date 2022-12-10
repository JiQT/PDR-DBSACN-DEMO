package dbscan

import dbscan.DBSCANLabeledPoint.Flag
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable.Queue

/**
 * A naive implementation of DBSCAN. It has O(n2) complexity
 * but uses no extra memory. This implementation is not used
 * by the parallel version of DBSCAN.
 *
 */
class LocalDBSCAN(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared = eps*eps

  //def samplePoint = Array(new DBSCANLabeledPoint(Vectors.dense(Array(0D, 0D))))

  def fit(points: Iterable[DBSCANPoint])= {

    logInfo(s"About to start fitting")

    val labeledPoints = points.map { new DBSCANLabeledPoint(_) }.toArray

    val totalClusters =
      labeledPoints
        .foldLeft(DBSCANLabeledPoint.Unknown)(
          (cluster, point) => {
            if (!point.visited) {
              point.visited = true
              val neighbors = findNeighbors(point, labeledPoints)
              //找出噪音点，cluster为0
              if (neighbors.size < minPoints) {
                point.flag = Flag.Noise
                cluster
                //找出核心点，新建cluster
              } else {
                expandCluster(point, neighbors, labeledPoints, cluster + 1)
                cluster + 1
              }
              //边界点
            } else {
              cluster
            }
          })

    logInfo(s"found: $totalClusters clusters")
    labeledPoints
  }

  private def findNeighbors(
    point: DBSCANPoint,
    all: Array[DBSCANLabeledPoint]): Iterable[DBSCANLabeledPoint] =
    all.view.filter(other => {
      point.distanceSquared(other) <= minDistanceSquared
    })

  def expandCluster(
    point: DBSCANLabeledPoint,
    neighbors: Iterable[DBSCANLabeledPoint],
    all: Array[DBSCANLabeledPoint],
    cluster: Int): Unit = {

    point.flag = Flag.Core
    point.cluster = cluster
    //将Iterable变为队列数据
    var allNeighbors = Queue(neighbors)

    while (allNeighbors.nonEmpty) {
      //数据依次出队判断
      allNeighbors.dequeue().foreach(neighbor => {
        if (!neighbor.visited) {

          neighbor.visited = true
          neighbor.cluster = cluster

          val neighborNeighbors = findNeighbors(neighbor, all)

          if (neighborNeighbors.size >= minPoints) {
            neighbor.flag = Flag.Core
            allNeighbors.enqueue(neighborNeighbors)
          } else {
            neighbor.flag = Flag.Border
          }

          if (neighbor.cluster == DBSCANLabeledPoint.Unknown) {
            neighbor.cluster = cluster
            neighbor.flag = Flag.Border
          }
        }
      })
    }
  }
}
