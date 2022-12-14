package dbscan
import archery.Box
import archery.Entry
import archery.Point
import archery.RTree

import scala.collection.mutable.Queue
import org.apache.spark.internal.Logging
import dbscan.DBSCANLabeledPoint.Flag

class LocalDBSCANArchery(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared = eps

  def fit(points: Iterable[DBSCANPoint]): Iterable[DBSCANLabeledPoint] = {

    val tree = points.foldLeft(RTree[DBSCANLabeledPoint]())(
      (tempTree, p) =>
        tempTree.insert(
          Entry(Point(p.x.toFloat, p.y.toFloat), new DBSCANLabeledPoint(p))))

    var cluster = DBSCANLabeledPoint.Unknown

    tree.entries.foreach(entry => {

      val point = entry.value

      if (!point.visited) {
        point.visited = true

        val neighbors = tree.search(toBoundingBox(point), inRange(point))

        if (neighbors.size < minPoints) {
          point.flag = Flag.Noise
        } else {
          cluster += 1
          expandCluster(point, neighbors, tree, cluster)
        }

      }

    })

    logDebug(s"total: $cluster")

    tree.entries.map(_.value).toIterable

  }

  private def expandCluster(
    point: DBSCANLabeledPoint,
    neighbors: Seq[Entry[DBSCANLabeledPoint]],
    tree: RTree[DBSCANLabeledPoint],
    cluster: Int): Unit = {

    point.flag = Flag.Core
    point.cluster = cluster

    val left = Queue(neighbors)

    while (left.nonEmpty) {

      left.dequeue().foreach(neighborEntry => {

        val neighbor = neighborEntry.value

        if (!neighbor.visited) {

          neighbor.visited = true
          neighbor.cluster = cluster

          val neighborNeighbors = tree.search(toBoundingBox(neighbor), inRange(neighbor))

          if (neighborNeighbors.size >= minPoints) {
            neighbor.flag = Flag.Core
            left.enqueue(neighborNeighbors)
          } else {
            neighbor.flag = Flag.Border
          }
        }

        if (neighbor.cluster == DBSCANLabeledPoint.Unknown) {
          neighbor.cluster = cluster
          neighbor.flag = Flag.Border
        }
      })

    }

  }

  private def inRange(point: DBSCANPoint)(entry: Entry[DBSCANLabeledPoint]): Boolean = {
    entry.value.distanceSquared(point) <= minDistanceSquared
  }

  private def toBoundingBox(point: DBSCANPoint): Box = {
    Box(
      (point.x - eps).toFloat,
      (point.y - eps).toFloat,
      (point.x + eps).toFloat,
      (point.y + eps).toFloat)
  }
  private def toBoundingBoxGeo(point: DBSCANPoint): Box = {
    val d=(eps / (6378137.0 * 1000))/(Math.cos(point.x* Math.PI / 180.0))*180.0 / Math.PI
    val c=(eps / (6378137.0 * 1000))/ Math.PI * 180.0
    Box(
      (point.x - c).toFloat,
      (point.y - d).toFloat,
      (point.x + c).toFloat,
      (point.y + d).toFloat)
  }
}
