package dbscan

import scala.collection.mutable.Queue

import org.apache.spark.internal.Logging
import dbscan.DBSCANLabeledPoint.Flag
import org.apache.spark.mllib.linalg.Vectors

/**
 * A naive implementation of DBSCAN. It has O(n2) complexity
 * but uses no extra memory. This implementation is not used
 * by the parallel version of DBSCAN.
 *
 */
class LocalDBSCANNaive(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared = eps

  def samplePoint = Array(new DBSCANLabeledPoint(Vectors.dense(Array(0D, 0D))))

  def fit(points: Iterable[DBSCANPoint]): Iterable[DBSCANLabeledPoint] = {

    logInfo(s"About to start fitting")

    val labeledPoints = points.map { new DBSCANLabeledPoint(_) }.toArray

    val totalClusters =
      labeledPoints
        .foldLeft(DBSCANLabeledPoint.Unknown)(
          (cluster, point) => {
            if (!point.visited) {
              point.visited = true

              val neighbors = findNeighbors(point, labeledPoints)
              //找出噪音点
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
      point.distanceSquared1(other) <= minDistanceSquared
    })

  def expandCluster(
    point: DBSCANLabeledPoint,
    neighbors: Iterable[DBSCANLabeledPoint],
    all: Array[DBSCANLabeledPoint],
    cluster: Int): Unit = {

    point.flag = Flag.Core
    point.cluster = cluster

    var allNeighbors = Queue(neighbors)

    while (allNeighbors.nonEmpty) {

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
