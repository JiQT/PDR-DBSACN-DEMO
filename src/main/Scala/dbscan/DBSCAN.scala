package dbscan

import org.apache.spark.internal.Logging
import dbscan.DBSCANLabeledPoint.Flag
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import scala.math.cos

object DBSCAN {

  def train(
    data: RDD[Vector],
    eps: Double,
    minPoints: Int,
    maxPointsPerPartition: Int): DBSCAN = {
      new DBSCAN(eps, minPoints, maxPointsPerPartition, null, null).train(data)
    }
}

class DBSCAN private (
  val eps: Double,
  val minPoints: Int,
  val maxPointsPerPartition: Int,
  //被 transient 标记的变量不会被序列化
  @transient val partitions: List[(Int, DBSCANRectangle)],
  @transient private val labeledPartitionedPoints: RDD[(Int, DBSCANLabeledPoint)])

  extends Serializable with Logging {

  type Margins = (DBSCANRectangle, DBSCANRectangle, DBSCANRectangle)
  type ClusterId = (Int, Int)

  def minimumRectangleSize = eps

  def labeledPoints: RDD[DBSCANLabeledPoint] = {
    labeledPartitionedPoints.values
  }

  private def train(vectors: RDD[Vector]): DBSCAN = {

    // generate the smallest rectangles that split the space
    // and count how many points are contained in each one of them
    val minimumRectanglesWithCount =
      vectors
        //DBSCANRectangle(x, y, x + minimumRectangleSize, y + minimumRectangleSize)
        .map(toMinimumBoundingRectangle)
        .map((_, 1))
        .aggregateByKey(0)(_ + _,_ + _)
        .collect()
        .toSet

//    find the best partitions for the data space
//    若分区内的点小于maxPointsPerPartition不执行该操作
    val localPartitions = EvenSplitPartitioner
      .partition(minimumRectanglesWithCount, maxPointsPerPartition, minimumRectangleSize)
    logDebug("Found partitions: ")
    localPartitions.foreach(p => logDebug(p.toString))

    // grow partitions to include eps
    //扩展分区以包含eps
    val localMargins =
      localPartitions
        .map({ case (p, _) => (p.shrink(eps), p, p.shrink(-eps)) })
        .zipWithIndex
    //将边界进行广播
    val margins = vectors.context.broadcast(localMargins)

    // assign each point to its proper partition
    //给每个点分配合适的分区，返回 分区id和点
    val duplicated = for {
      point <- vectors.map(DBSCANPoint)
      ((inner, main, outer), id) <- margins.value
      if outer.contains(point)
    } yield (id, point)

    val numOfPartitions = localPartitions.size

    // perform local dbscan
    //并行执行每个分区的dbscan
    val clustered =
      duplicated
        .groupByKey(numOfPartitions)
        .flatMapValues(points =>
          new LocalDBSCANNaive(eps, minPoints).fit(points))
        .cache()

    // find all candidate points for merging clusters and group them
    val mergePoints =
      clustered
        .flatMap({
          case (partition, point) =>
            margins.value
              .filter({
                case ((inner, main, _), _) => main.contains(point) && !inner.almostContains(point)
              })
              .map({
                case (_, newPartition) => (newPartition, (partition, point))
              })
        })
        .groupByKey()

    logDebug("About to find adjacencies")
    // find all clusters with aliases from merging candidates
    //partition, point.cluster
    val adjacencies =
      mergePoints
        .flatMapValues(findAdjacencies)
        .values
        .collect()

    // generated adjacency graph
    val adjacencyGraph = adjacencies.foldLeft(DBSCANGraph[ClusterId]()) {
      case (graph, (from, to)) => graph.connect(from, to)
    }

    logDebug("About to find all cluster ids")
    // find all cluster ids
    val localClusterIds =
      clustered
        .filter({ case (_, point) => point.flag != Flag.Noise })
        .mapValues(_.cluster)
        .distinct()
        .collect()
        .toList

    // assign a global Id to all clusters, where connected clusters get the same id
    val (total, clusterIdToGlobalId) = localClusterIds.foldLeft((0, Map[ClusterId, Int]())) {
      case ((id, map), clusterId) => {

        map.get(clusterId) match {
          case None => {
            val nextId = id + 1
            val connectedClusters = adjacencyGraph.getConnected(clusterId) + clusterId
            logDebug(s"Connected clusters $connectedClusters")
            val toadd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toadd)
          }
          case Some(x) =>
            (id, map)
        }
      }
    }

    logDebug("Global Clusters")
    clusterIdToGlobalId.foreach(e => logDebug(e.toString))
    logInfo(s"Total Clusters: ${localClusterIds.size}, Unique: $total")

    val clusterIds = vectors.context.broadcast(clusterIdToGlobalId)

    logDebug("About to relabel inner points")
    // relabel non-duplicated points
    val labeledInner =
      clustered
        .filter(isInnerPoint(_, margins.value))
        .map {
          case (partition, point) => {

            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }
            (partition, point)
          }
        }

    logDebug("About to relabel outer points")
    // de-duplicate and label merge points
    val labeledOuter =
      mergePoints.flatMapValues(partition => {
        partition.foldLeft(Map[DBSCANPoint, DBSCANLabeledPoint]())({
          case (all, (partition, point)) =>

            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }

            all.get(point) match {
              case None => all + (point -> point)
              case Some(prev) => {
                // override previous entry unless new entry is noise
                if (point.flag != Flag.Noise) {
                  prev.flag = point.flag
                  prev.cluster = point.cluster
                }
                all
              }
            }

        }).values
      })

    val finalPartitions = localMargins.map {
      case ((_, p, _), index) => (index, p)
    }

    logDebug("Done")

    new DBSCAN(
      eps,
      minPoints,
      maxPointsPerPartition,
      finalPartitions,
      labeledInner.union(labeledOuter))
  }

  /**
   * Find the appropriate label to the given `vector`
   *
   * This method is not yet implemented
   */
  def predict(vector: Vector): DBSCANLabeledPoint = {
    throw new NotImplementedError
  }

  private def isInnerPoint(
    entry: (Int, DBSCANLabeledPoint),
    margins: List[(Margins, Int)]): Boolean = {
    entry match {
      case (partition, point) =>
        val ((inner, _, _), _) = margins.filter({
          case (_, id) => id == partition
        }).head

        inner.almostContains(point)
    }
  }

  private def findAdjacencies(
    partition: Iterable[(Int, DBSCANLabeledPoint)]): Set[((Int, Int), (Int, Int))] = {

    val zero = (Map[DBSCANPoint, ClusterId](), Set[(ClusterId, ClusterId)]())

    val (seen, adjacencies) = partition.foldLeft(zero)({

      case ((seen, adjacencies), (partition, point)) =>

        // noise points are not relevant for adjacencies
        if (point.flag == Flag.Noise) {
          (seen, adjacencies)
        } else {

          val clusterId = (partition, point.cluster)

          seen.get(point) match {
            case None                => (seen + (point -> clusterId), adjacencies)
            case Some(prevClusterId) => (seen, adjacencies + ((prevClusterId, clusterId)))
          }
        }
    })
    adjacencies
  }

  private def toMinimumBoundingRectangle(vector: Vector): DBSCANRectangle = {
    val point = DBSCANPoint(vector)

    val x = corner(point.x)
    val y = corner(point.y)
    //x，y往右上方平移minimumRectangleSize个单位
//    val d = minimumRectangleSize / (6378137.0 * 1000)
//    val cx=cos(y* Math.PI / 180.0)
//    val x1= d / cx * 180.0 / Math.PI+x
//    val y1= d / Math.PI * 180.0+y
    //DBSCANRectangle(x, y, x1, y1)
    DBSCANRectangle(x, y, x + minimumRectangleSize, y + minimumRectangleSize)
  }

  private def corner(p: Double): Double = {
    //归一化坐标
    (shiftIfNegative(p) / minimumRectangleSize).intValue * minimumRectangleSize
  }

  private def shiftIfNegative(p: Double): Double =
    if (p < 0) p - minimumRectangleSize else p
}
