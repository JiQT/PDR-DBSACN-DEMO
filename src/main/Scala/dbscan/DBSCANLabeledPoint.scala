package dbscan

import org.apache.spark.mllib.linalg.Vector

/**
 * 标记点的相关参数
 */
object DBSCANLabeledPoint {
  val Unknown = 0
  object Flag extends Enumeration {
    type Flag = Value
    //Enumeration枚举（边界点，核心点，噪音点，未标记点）
    val Border, Core, Noise, NotFlagged = Value
  }
}

class DBSCANLabeledPoint(vector: Vector) extends DBSCANPoint(vector) {

  def this(point: DBSCANPoint) = this(point.vector)
  //初始化
  var flag = DBSCANLabeledPoint.Flag.NotFlagged
  var cluster = DBSCANLabeledPoint.Unknown
  var visited = false

  override def toString(): String = {
    s"$vector,$cluster,$flag"
  }
}
